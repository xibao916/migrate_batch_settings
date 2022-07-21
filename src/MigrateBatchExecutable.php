<?php

namespace Drupal\migrate_batch_settings;

use Drupal\migrate\MigrateMessage;
use Drupal\migrate\MigrateMessageInterface;
use Drupal\migrate\Plugin\MigrationInterface;
use Drupal\migrate_tools\MigrateBatchExecutable as Migrate_toolsMigrateBatchExecutable;
use Drupal\migrate\Event\MigrateEvents;
use Drupal\migrate\Event\MigrateImportEvent;
use Drupal\migrate\Event\MigratePostRowSaveEvent;
use Drupal\migrate\Event\MigratePreRowSaveEvent;
use Drupal\migrate\Exception\RequirementsException;
use Drupal\migrate\MigrateException;
use Drupal\migrate\MigrateSkipRowException;
use Drupal\migrate\Plugin\MigrateIdMapInterface;

/**
 * Defines a migrate executable class for batch migrations through UI.
 */
class MigrateBatchExecutable extends Migrate_toolsMigrateBatchExecutable {

  /**
   * Migration plugin batch size.
   *
   * @var array
   */
  protected $batchSize = 0;

  /**
   * The handle source.
   *
   * @var \Drupal\migrate\Plugin\MigrateSourceInterface
   */
  protected $handleSource;

  /**
   * {@inheritdoc}
   */
  public function __construct(MigrationInterface $migration, MigrateMessageInterface $message, array $options = []) {

    if (isset($options['batch_size'])) {
      $this->batchSize = $options['batch_size'];
    }

    parent::__construct($migration, $message, $options);
  }

  /**
   * Setup batch operations for running the migration.
   */
  public function batchImport() {
    // Create the batch operations for each migration that needs to be executed.
    // This includes the migration for this executable, but also the dependent
    // migrations.
    $operations = $this->batchOperations([$this->migration], 'import', [
      'limit' => $this->itemLimit,
      'update' => $this->updateExistingRows,
      'force' => $this->checkDependencies,
      'sync' => $this->syncSource,
      'configuration' => $this->configuration,
      'batch_size' => $this->batchSize,
    ]);

    if (count($operations) > 0) {
      $batch = [
        'operations' => $operations,
        'title' => $this->t('Migrating %migrate', ['%migrate' => $this->migration->label()]),
        'init_message' => $this->t('Start migrating %migrate', ['%migrate' => $this->migration->label()]),
        'progress_message' => $this->t('Migrating %migrate', ['%migrate' => $this->migration->label()]),
        'error_message' => $this->t('An error occurred while migrating %migrate.', ['%migrate' => $this->migration->label()]),
        'finished' => '\Drupal\migrate_tools\MigrateBatchExecutable::batchFinishedImport',
      ];

      batch_set($batch);
    }
  }

  /**
   * Helper to generate the batch operations for importing migrations.
   *
   * @param \Drupal\migrate\Plugin\MigrationInterface[] $migrations
   *   The migrations.
   * @param string $operation
   *   The batch operation to perform.
   * @param array $options
   *   The migration options.
   *
   * @return array
   *   The batch operations to perform.
   */
  protected function batchOperations(array $migrations, $operation, array $options = []) {
    $operations = [];
    foreach ($migrations as $id => $migration) {

      if (!empty($options['update'])) {
        $migration->getIdMap()->prepareUpdate();
      }

      if (!empty($options['force'])) {
        $migration->set('requirements', []);
      }
      else {
        $dependencies = $migration->getMigrationDependencies();
        if (!empty($dependencies['required'])) {
          $required_migrations = $this->migrationPluginManager->createInstances($dependencies['required']);
          // For dependent migrations will need to be migrate all items.
          $operations = array_merge($operations, $this->batchOperations($required_migrations, $operation, [
            'limit' => 0,
            'update' => $options['update'],
            'force' => $options['force'],
            'sync' => $options['sync'],
          ]));
        }
      }

      $operations[] = [
        '\Drupal\migrate_batch_settings\MigrateBatchExecutable::batchProcessImport',
        [$migration->id(), $options],
      ];
    }

    return $operations;
  }

  /**
   * Batch 'operation' callback.
   *
   * @param string $migration_id
   *   The migration id.
   * @param array $options
   *   The batch executable options.
   * @param array|\DrushBatchContext $context
   *   The sandbox context.
   */
  public static function batchProcessImport($migration_id, array $options, &$context) {
    if (empty($context['sandbox'])) {
      $context['finished'] = 0;
      $context['sandbox'] = [];
      $context['sandbox']['total'] = 0;
      $context['sandbox']['counter'] = 0;
      $context['sandbox']['batch_limit'] = 0;
      $context['sandbox']['batch_id'] = 0;
      $context['sandbox']['operation'] = MigrateBatchExecutable::BATCH_IMPORT;
    }

    // Prepare the migration executable.
    $message = new MigrateMessage();
    /** @var \Drupal\migrate\Plugin\MigrationInterface $migration */
    $migration = \Drupal::getContainer()->get('plugin.manager.migration')->createInstance($migration_id, isset($options['configuration']) ? $options['configuration'] : []);
    unset($options['configuration']);

    // Each batch run we need to reinitialize the counter for the migration.
    if (!empty($options['limit']) && isset($context['results'][$migration->id()]['@numitems'])) {
      $options['limit'] = $options['limit'] - $context['results'][$migration->id()]['@numitems'];
    }

    $executable = new MigrateBatchExecutable($migration, $message, $options);

    if (empty($context['sandbox']['total'])) {
      $context['sandbox']['total'] = $executable->getSource()->count();
      $context['sandbox']['batch_limit'] = $executable->calculateBatchLimit($context);
      $context['results'][$migration->id()] = [
        '@numitems' => 0,
        '@created' => 0,
        '@updated' => 0,
        '@failures' => 0,
        '@ignored' => 0,
        '@name' => $migration->id(),
      ];
    }

    // Every iteration, we reset out batch counter.
    $context['sandbox']['batch_counter'] = 0;

    // Make sure we know our batch context.
    $executable->setBatchContext($context);
    $session = \Drupal::request()->getSession();
    $key = 'migrate_' . $migration_id;
    $session->set($key, [
      'batch_size' => $options['batch_size'],
      'batch_id' => $context['sandbox']['batch_id'],
    ]);
    // Do the import.
    $result = $executable->import();
    // Clear tmp file.
    $file_key = $key . '_files';
    $current_file_path = $session->get($file_key)[$context['sandbox']['batch_id']];
    \Drupal::service('file_system')->deleteRecursive($current_file_path);
    $session->remove($key);
    $context['sandbox']['batch_id']++;

    // Store the result; will need to combine the results of all our iterations.
    $context['results'][$migration->id()] = [
      '@numitems' => $context['results'][$migration->id()]['@numitems'] + $executable->getProcessedCount(),
      '@created' => $context['results'][$migration->id()]['@created'] + $executable->getCreatedCount(),
      '@updated' => $context['results'][$migration->id()]['@updated'] + $executable->getUpdatedCount(),
      '@failures' => $context['results'][$migration->id()]['@failures'] + $executable->getFailedCount(),
      '@ignored' => $context['results'][$migration->id()]['@ignored'] + $executable->getIgnoredCount(),
      '@name' => $migration->id(),
    ];

    // Do some housekeeping.
    if (
      $result != MigrationInterface::RESULT_INCOMPLETE
    ) {
      if ($options['batch_size'] > 0 && $context['sandbox']['batch_id'] == 1) {
        $context['finished'] = ((float) $context['sandbox']['counter'] / (float) $context['sandbox']['total']);
        $context['message'] = t('Importing %migration (@percent%) Imported: @counter.', [
          '%migration' => $migration->label(),
          '@percent' => sprintf('%.2f', ($context['finished'] * 100)),
          '@counter' => $context['sandbox']['counter'],
        ]);
      }
      else {
        $context['finished'] = 1;
        // Clear old files data.
        \Drupal::service('file_system')->deleteRecursive(BATCH_BASEDIR . $migration_id . '/');
        $session->remove($file_key);
      }
    }
    else {
      $context['sandbox']['counter'] = $context['results'][$migration->id()]['@numitems'];
      if ($context['sandbox']['counter'] <= $context['sandbox']['total']) {
        $context['finished'] = ((float) $context['sandbox']['counter'] / (float) $context['sandbox']['total']);
        $context['message'] = t('Importing %migration (@percent%) Imported: @counter.', [
          '%migration' => $migration->label(),
          '@percent' => sprintf('%.2f', ($context['finished'] * 100)),
          '@counter' => $context['sandbox']['counter'],
        ]);
      }
    }
  }

  /**
   * Returns the handle source.
   *
   * Makes sure source is initialized based on migration settings.
   *
   * @return \Drupal\migrate\Plugin\MigrateSourceInterface
   *   The source.
   */
  protected function getHandleSource() {
    $this->handleSource = \Drupal::service('plugin.manager.migrate.source')->createInstance('csv_batch', $this->migration->getSourceConfiguration(), $this->migration);
    return $this->handleSource;
  }

  /**
   * {@inheritdoc}
   */
  public function import() {
    // Only begin the import operation if the migration is currently idle.
    if ($this->migration->getStatus() !== MigrationInterface::STATUS_IDLE) {
      $this->message->display($this->t('Migration @id is busy with another operation: @status',
        [
          '@id' => $this->migration->id(),
          '@status' => $this->t($this->migration->getStatusLabel()),
        ]), 'error');
      return MigrationInterface::RESULT_FAILED;
    }
    $this->getEventDispatcher()->dispatch(new MigrateImportEvent($this->migration, $this->message), MigrateEvents::PRE_IMPORT);

    // Knock off migration if the requirements haven't been met.
    try {
      $this->migration->checkRequirements();
    }
    catch (RequirementsException $e) {
      $this->message->display(
        $this->t(
          'Migration @id did not meet the requirements. @message @requirements',
          [
            '@id' => $this->migration->id(),
            '@message' => $e->getMessage(),
            '@requirements' => $e->getRequirementsString(),
          ]
        ),
        'error'
      );

      return MigrationInterface::RESULT_FAILED;
    }

    $this->migration->setStatus(MigrationInterface::STATUS_IMPORTING);
    $source = $this->getHandleSource();

    try {
      $source->rewind();
    }
    catch (\Exception $e) {
      $this->message->display(
        $this->t('Migration failed with source plugin exception: @e in @file line @line', [
          '@e' => $e->getMessage(),
          '@file' => $e->getFile(),
          '@line' => $e->getLine(),
        ]), 'error');
      $this->migration->setStatus(MigrationInterface::STATUS_IDLE);
      return MigrationInterface::RESULT_FAILED;
    }

    // Get the process pipeline.
    $pipeline = FALSE;
    if ($source->valid()) {
      try {
        $pipeline = $this->migration->getProcessPlugins();
      }
      catch (MigrateException $e) {
        $row = $source->current();
        $this->sourceIdValues = $row->getSourceIdValues();
        $this->getIdMap()->saveIdMapping($row, [], $e->getStatus());
        $this->saveMessage($e->getMessage(), $e->getLevel());
      }
    }

    $return = MigrationInterface::RESULT_COMPLETED;
    if ($pipeline) {
      $id_map = $this->getIdMap();
      $destination = $this->migration->getDestinationPlugin();
      while ($source->valid()) {
        $row = $source->current();
        $this->sourceIdValues = $row->getSourceIdValues();

        try {
          foreach ($pipeline as $destination_property_name => $plugins) {
            $this->processPipeline($row, $destination_property_name, $plugins, NULL);
          }
          $save = TRUE;
        }
        catch (MigrateException $e) {
          $this->getIdMap()->saveIdMapping($row, [], $e->getStatus());
          $msg = sprintf("%s:%s: %s", $this->migration->getPluginId(), $destination_property_name, $e->getMessage());
          $this->saveMessage($msg, $e->getLevel());
          $save = FALSE;
        }
        catch (MigrateSkipRowException $e) {
          if ($e->getSaveToMap()) {
            $id_map->saveIdMapping($row, [], MigrateIdMapInterface::STATUS_IGNORED);
          }
          if ($message = trim($e->getMessage())) {
            $msg = sprintf("%s:%s: %s", $this->migration->getPluginId(), $destination_property_name, $message);
            $this->saveMessage($msg, MigrationInterface::MESSAGE_INFORMATIONAL);
          }
          $save = FALSE;
        }

        if ($save) {
          try {
            $this->getEventDispatcher()
              ->dispatch(new MigratePreRowSaveEvent($this->migration, $this->message, $row), MigrateEvents::PRE_ROW_SAVE);
            $destination_ids = $id_map->lookupDestinationIds($this->sourceIdValues);
            $destination_id_values = $destination_ids ? reset($destination_ids) : [];
            $destination_id_values = $destination->import($row, $destination_id_values);
            $this->getEventDispatcher()
              ->dispatch(new MigratePostRowSaveEvent($this->migration, $this->message, $row, $destination_id_values), MigrateEvents::POST_ROW_SAVE);
            if ($destination_id_values) {
              // We do not save an idMap entry for config.
              if ($destination_id_values !== TRUE) {
                $id_map->saveIdMapping($row, $destination_id_values, $this->sourceRowStatus, $destination->rollbackAction());
              }
            }
            else {
              $id_map->saveIdMapping($row, [], MigrateIdMapInterface::STATUS_FAILED);
              if (!$id_map->messageCount()) {
                $message = $this->t('New object was not saved, no error provided');
                $this->saveMessage($message);
                $this->message->display($message);
              }
            }
          }
          catch (MigrateException $e) {
            $this->getIdMap()->saveIdMapping($row, [], $e->getStatus());
            $this->saveMessage($e->getMessage(), $e->getLevel());
          }
          catch (\Exception $e) {
            $this->getIdMap()
              ->saveIdMapping($row, [], MigrateIdMapInterface::STATUS_FAILED);
            $this->handleException($e);
          }
        }

        $this->sourceRowStatus = MigrateIdMapInterface::STATUS_IMPORTED;

        // Check for memory exhaustion.
        if (($return = $this->checkStatus()) != MigrationInterface::RESULT_COMPLETED) {
          break;
        }

        // If anyone has requested we stop, return the requested result.
        if ($this->migration->getStatus() == MigrationInterface::STATUS_STOPPING) {
          file_put_contents('while_end.txt', $return . '-- stop');
          $return = $this->migration->getInterruptionResult();
          $this->migration->clearInterruptionResult();
          break;
        }

        try {
          $source->next();
        }
        catch (\Exception $e) {
          $this->message->display(
            $this->t('Migration failed with source plugin exception: @e in @file line @line', [
              '@e' => $e->getMessage(),
              '@file' => $e->getFile(),
              '@line' => $e->getLine(),
            ]), 'error');
          $this->migration->setStatus(MigrationInterface::STATUS_IDLE);
          return MigrationInterface::RESULT_FAILED;
        }
      }
    }

    $this->getEventDispatcher()->dispatch(new MigrateImportEvent($this->migration, $this->message), MigrateEvents::POST_IMPORT);
    $this->migration->setStatus(MigrationInterface::STATUS_IDLE);
    return $return;
  }

  /**
   * Calculates how much a single batch iteration will handle.
   *
   * @param array|\DrushBatchContext $context
   *   The sandbox context.
   *
   * @return float
   *   The batch limit.
   */
  public function calculateBatchLimit($context) {
    // @TODO Maybe we need some other more sophisticated logic here?
    if ($this->batchSize > 0) {
      return $this->batchSize;
    }
    return ceil($context['sandbox']['total'] / 100);
  }

}
