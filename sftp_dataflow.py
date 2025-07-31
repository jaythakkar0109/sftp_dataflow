import os
import sys
import logging
import paramiko
import pendulum
import fnmatch
import json
from typing import List, Dict, Any
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
import tempfile


class SftpFileHandlerOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--project_id', default=os.environ.get('GCP_PROJECT_ID'), help='GCP Project ID')
        parser.add_argument('--gcs_bucket', default=os.environ.get('GCS_BUCKET'), help='GCS Bucket Name')
        parser.add_argument('--sftp_host', default=os.environ.get('SFTP_HOST'), help='SFTP Host')
        parser.add_argument('--sftp_port', type=int, default=int(os.environ.get('SFTP_PORT', 22)), help='SFTP Port')
        parser.add_argument('--sftp_username', default=os.environ.get('SFTP_USERNAME'), help='SFTP Username')
        parser.add_argument('--sftp_password', default=os.environ.get('SFTP_PASSWORD'), help='SFTP Password')
        parser.add_argument('--source_system', default=os.environ.get('SFTP_SOURCE_SYSTEM'), help='Source System')
        parser.add_argument('--remote_directory', default=os.environ.get('SFTP_REMOTE_DIRECTORY'), help='Remote Directory')
        parser.add_argument('--base_file_name', default=os.environ.get('SFTP_BASE_FILE_NAME'), help='Base File Name')
        parser.add_argument('--file_extension', default=os.environ.get('SFTP_FILE_EXTENSION'), help='File Extension')


class SftpToGcsDoFn(beam.DoFn):
    """DoFn that matches the exact logic from your original SftpFileHandler class."""
    
    def __init__(self, options):
        self.options = options
        self.ssh_client = None
        self.sftp_client = None
        self.storage_client = None
        self.logger = None

    def setup(self):
        """Initialize connections once per worker - matches your connect_sftp method."""
        # Setup logging exactly like your original class
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
        try:
            # Initialize SFTP connection - matches your connect_sftp method exactly
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.load_system_host_keys()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            self.ssh_client.connect(
                hostname=self.options.sftp_host,
                port=self.options.sftp_port,
                username=self.options.sftp_username,
                password=self.options.sftp_password,
                timeout=120  # Same timeout as your original
            )
            
            self.sftp_client = self.ssh_client.open_sftp()
            
            # Initialize GCS client
            self.storage_client = storage.Client(self.options.project_id)
            
            self.logger.info({"message": "SFTP connection established successfully"})
            
        except Exception as e:
            self.logger.exception({"message": "Failed to establish SFTP connection", "error": str(e)})
            raise

    def process(self, file_name):
        """Process each file - matches your original get_files logic exactly."""
        remote_path = f"{self.options.remote_directory}/{file_name}" if self.options.remote_directory else file_name
        
        try:
            # Read file content exactly like your original code using sftp_client.file()
            with self.sftp_client.file(remote_path, 'r') as remote_file:
                file_content = remote_file.read()
            
            # Upload to GCS using your exact upload_object_from_file logic
            filepath = f"{self.options.remote_directory}/{file_name}" if self.options.remote_directory else file_name
            self.logger.info({"message": "Uploading file to GCS", "source": self.options.source_system, "path": filepath})
            
            blob = self.storage_client.bucket(self.options.gcs_bucket).blob(filepath)
            blob.upload_from_string(file_content.decode())  # Decode like your original
            
            self.logger.info({"message": "Successfully uploaded to GCS", "object": file_name, "path": filepath})
            
            # Return file details in the exact same format as your original
            file_details = {
                file_name: {
                    'remote_path': remote_path,
                    'upload_time': pendulum.now('US/Central').to_datetime_string(),
                    'status': 'success'
                }
            }
            
            yield file_details
            
        except Exception as file_error:
            self.logger.error({"message": "Failed to process file", "file_name": file_name, "error": str(file_error)})
            
            file_details = {
                file_name: {
                    'remote_path': remote_path,
                    'upload_time': pendulum.now('US/Central').to_datetime_string(),
                    'status': 'failed',
                    'error': str(file_error)
                }
            }
            
            yield file_details

    def teardown(self):
        """Close connections - matches your close_connection method exactly."""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.logger.info({"message": "SFTP connection closed"})
            if self.ssh_client:
                self.ssh_client.close()
                self.logger.info({"message": "SSH connection closed"})
        except Exception as e:
            self.logger.exception({"message": "Error closing SFTP/SSH connections", "error": str(e)})


def list_sftp_files(options) -> List[str]:
    """List files in SFTP directory - matches your search_files_by_pattern method exactly."""
    ssh_client = None
    sftp_client = None
    try:
        # Create temporary SFTP connection for file listing - same logic as your connect_sftp
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=options.sftp_host,
            port=options.sftp_port,
            username=options.sftp_username,
            password=options.sftp_password,
            timeout=120
        )
        sftp_client = ssh_client.open_sftp()
        
        # List files exactly like your list_remote_files method
        directory = options.remote_directory or "."
        files = sftp_client.listdir(directory)
        logging.info({"message": "Files in directory", "directory": directory, "files": files})
        
        # Filter by pattern exactly like your search_files_by_pattern method
        file_pattern = f"{options.base_file_name}*.{options.file_extension}"
        matching_files = [f for f in files if fnmatch.fnmatch(f, file_pattern)]
        
        logging.info({"message": "Files matching pattern", "pattern": file_pattern, "matching_files": matching_files})
        return matching_files
        
    except Exception as e:
        logging.exception({"message": "Failed to list remote files", "error": str(e)})
        return []
    finally:
        # Clean up connections - matches your close_connection method
        try:
            if sftp_client:
                sftp_client.close()
            if ssh_client:
                ssh_client.close()
        except Exception as e:
            logging.exception({"message": "Error closing SFTP/SSH connections", "error": str(e)})


def run(argv=None):
    """Run the Dataflow pipeline - matches your main() function logic."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    pipeline_options = PipelineOptions(argv)
    options = pipeline_options.view_as(SftpFileHandlerOptions)

    # Validate required options exactly like your _get_config_value method
    required_options = ['project_id', 'gcs_bucket', 'sftp_host', 'sftp_username', 'sftp_password', 
                       'source_system', 'remote_directory', 'base_file_name', 'file_extension']
    for opt in required_options:
        if not getattr(options, opt):
            error_msg = f"Missing required environment variable: SFTP_{opt.upper()}"
            logger.error({"message": "Required environment variable not set", "env_key": f"SFTP_{opt.upper()}"})
            raise ValueError(error_msg)

    logger.info({"message": "Starting SFTP file retrieval"})
    load_start_time = pendulum.now('US/Central').to_datetime_string()

    # Get list of files to process - matches your get_files method
    files = list_sftp_files(options)
    
    if not files:
        file_pattern = f"{options.base_file_name}*.{options.file_extension}"
        error_msg = f"No files found matching pattern: {file_pattern}"
        logger.error({"message": error_msg})
        raise Exception(error_msg)  # Match your original behavior

    logger.info({"message": f"Found {len(files)} files to process", "files": files})

    # Define and run the Beam pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        
        # Process files
        results = (p
                  | 'Create File List' >> beam.Create(files)
                  | 'Process Files to GCS' >> beam.ParDo(SftpToGcsDoFn(options))
                  )
        
        # Collect and validate results like your original code
        def validate_results(file_detail_dict):
            """Validate processing results - matches your original success checking."""
            for file_name, details in file_detail_dict.items():
                if details['status'] == 'success':
                    logger.info({"message": "Successfully processed file", "file_name": file_name})
                else:
                    logger.error({"message": "Failed to process file", "file_name": file_name, "error": details.get('error', 'Unknown error')})
            return file_detail_dict
        
        # Log results exactly like your original code
        results | 'Validate and Log Results' >> beam.Map(validate_results)
        
    logger.info({"message": "Files processing completed", "load_start_time": load_start_time})


def main():
    """Main function to run the pipeline with proper error handling."""
    try:
        # Get pipeline arguments from command line or environment
        pipeline_args = []
        
        # Only add arguments that have values
        env_mappings = {
            '--project_id': 'GCP_PROJECT_ID',
            '--gcs_bucket': 'GCS_BUCKET',
            '--sftp_host': 'SFTP_HOST',
            '--sftp_port': 'SFTP_PORT',
            '--sftp_username': 'SFTP_USERNAME',
            '--sftp_password': 'SFTP_PASSWORD',
            '--source_system': 'SFTP_SOURCE_SYSTEM',
            '--remote_directory': 'SFTP_REMOTE_DIRECTORY',
            '--base_file_name': 'SFTP_BASE_FILE_NAME',
            '--file_extension': 'SFTP_FILE_EXTENSION',
        }
        
        for arg, env_var in env_mappings.items():
            value = os.environ.get(env_var)
            if value:
                pipeline_args.extend([arg, value])
        
        # Add Dataflow-specific arguments
        pipeline_args.extend([
            '--runner', 'DataflowRunner',
            '--region', os.environ.get('DATAFLOW_REGION', 'us-central1'),
            '--staging_location', f"gs://{os.environ.get('GCS_BUCKET')}/staging",
            '--temp_location', f"gs://{os.environ.get('GCS_BUCKET')}/temp",
            '--num_workers', os.environ.get('DATAFLOW_NUM_WORKERS', '2'),
            '--max_num_workers', os.environ.get('DATAFLOW_MAX_WORKERS', '10'),
            '--autoscaling_algorithm', 'THROUGHPUT_BASED',
            '--worker_machine_type', os.environ.get('DATAFLOW_MACHINE_TYPE', 'n1-standard-1'),
        ])
        
        # Add setup file if it exists
        if os.path.exists('./setup.py'):
            pipeline_args.extend(['--setup_file', './setup.py'])
        
        logging.info(f"Starting pipeline with args: {pipeline_args}")
        run(pipeline_args)
        logging.info("Pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise  # Re-raise to ensure proper exit code


if __name__ == "__main__":
    main()
