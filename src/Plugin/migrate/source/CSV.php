<?php

namespace Drupal\migrate_batch_settings\Plugin\migrate\source;

use Drupal\Core\File\FileSystemInterface;
use Drupal\migrate_source_csv\Plugin\migrate\source\CSV as SourceCSV;
use League\Csv\Reader;

/**
 * Source for CSV files.
 *
 * @MigrateSource(
 *   id = "csv_batch",
 *   source_module = "migrate_batch_settings"
 * )
 */
class CSV extends SourceCSV {

  /**
   * The base directory to store our temporary batched files.
   *
   * @var string
   */
  protected $baseDirectory = 'private://migration_batch/';

  /**
   * The spl object.
   *
   * @var \SplFileObject
   */
  protected $splFileObject;

  /**
   * Construct a new CSV reader.
   *
   * @return \League\Csv\Reader
   *   The reader.
   */
  protected function createReader() {
    if (!file_exists($this->configuration['path'])) {
      throw new \RuntimeException(sprintf('File "%s" was not found.', $this->configuration['path']));
    }
    $session = \Drupal::request()->getSession();
    $key = 'migrate_' . $this->migration->id();
    if ($session->has($key) && $session->get($key)['batch_size'] > 0) {
      $file_key = $key . '_files';
      $options = $session->get($key);
      if ($session->has($file_key) && $session->get($file_key)) {
        $files = $session->get($file_key);
      }
      else {
        $files = $this->createTmpFile($options['batch_size']);
        $session->set($file_key, $files);
      }
      $file_path = $files[$options['batch_id']];
      $csv = fopen($file_path, 'r');
    }
    else {
      $csv = fopen($this->configuration['path'], 'r');
    }
    if (!$csv) {
      throw new \RuntimeException(sprintf('File "%s" could not be opened.', $this->configuration['path']));
    }
    return Reader::createFromStream($csv);
  }

  /**
   * Creates batch tmp file.
   *
   * @param int $batch_size
   *   File max line number.
   *
   * @return array
   *   The file paths.
   */
  private function createTmpFile($batch_size = 100) {
    // Increase the time of the cut file operation.
    set_time_limit(300);

    $spl_object = $this->getSplFileObject();
    $spl_object->rewind();
    $header = $spl_object->fgetcsv();
    // Skip imported lines.
    $imported_count = $this->migration->getIdMap()->importedCount();
    // Skip header line.
    if ($imported_count > 0) {
      $spl_object->seek($imported_count);
    }

    $files = [];
    $line = 0;
    $batch_id = 0;
    $file_handle = $this->createBatchedFile($files, $batch_id);
    fputcsv($file_handle, $header);
    while (!$spl_object->eof()) {
      if ($line >= $batch_size || ($batch_id == 0 && $line >= 1)) {
        $batch_id++;
        $line = 0;
        // Close the existing file and create a new one.
        fclose($file_handle);
        $file_handle = $this->createBatchedFile($files, $batch_id);
        fputcsv($file_handle, $header);
      }
      fputcsv($file_handle, $spl_object->fgetcsv());
      $spl_object->next();
      $line++;
    }
    if ($file_handle) {
      fclose($file_handle);
    }
    return $files;
  }

  /**
   * Creates a batched file via Drupal's unmanaged files.
   *
   * @param array $files
   *   An array of the files created. Passed by reference so we may add to it.
   * @param string $batch_id
   *   The batch ID corresponding to this file.
   *
   * @return bool|resource
   *   The resource for the newly created file.
   */
  private function createBatchedFile(array &$files, $batch_id) {
    $file_system = \Drupal::service('file_system');
    $migration_id = $this->migration->id();
    $base_directory = $this->baseDirectory . $migration_id . '/';
    $file_system->prepareDirectory($base_directory, FileSystemInterface::CREATE_DIRECTORY);
    $tmp_file_name = $migration_id . '_' . $batch_id;
    $destination = $base_directory . $tmp_file_name . '.csv';
    $file_system->saveData('', $destination, FileSystemInterface::EXISTS_REPLACE);
    $file_path = $file_system->realpath($destination);
    $files[] = $file_path;
    return fopen($file_path, 'w');
  }

  /**
   * {@inheritdoc}
   */
  protected function getSplFileObject() {
    if (!isset($this->splFileObject)) {
      $this->splFileObject = new \SplFileObject($this->configuration['path'], 'rb');
    }

    return $this->splFileObject;
  }

}
