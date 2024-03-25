from datetime import datetime
import gzip

from airflow.operators.python import PythonOperator
from common.efsg_dag import EFSGDag
from definitions.enums import DagIDs

from airflow.providers.amazon.aws.operators.s3 import S3Hook


s3_hook = S3Hook('aws_default')
BUCKET = 'my-s3-bucket'
BACTH_DATE = '2021-09-01'   # hardcode for testing, it should be {{ds}} in production


# Task Level Definition
default_args = dict(
    start_date=datetime(year=2021, month=8, day=31),
    retries=0,
)

with EFSGDag(
    dag_id=DagIDs.data_test_dag,
    default_args=default_args,
    schedule='0/30 * * * *',
    catchup=False,
    max_active_runs=1,
) as dag:

    def get_event_list() -> list:
        event_list = s3_hook.list_prefixes(
            bucket_name=BUCKET,
            prefix='source/',
            delimiter='/',
        )

        return [event.split('/')[1] for event in event_list]

    def check_target_zip_existance(
        event: str,
        date: str,
        hour: str
    ) -> bool:
        print('>>> ', f'target/{event}/{date}/{hour}.csv.gz')
        return s3_hook.check_for_key(
            bucket_name=BUCKET,
            key=f'target/{event}/{date}/{hour}.csv.gz',
        )

    def get_keys_from_prefix(
        event: str,
        event_date: str,
        from_datetime: str = None,
    ) -> list:
        return s3_hook.list_keys(
            bucket_name=BUCKET,
            prefix=f'source/{event}/{event_date}', 
            from_datetime=datetime.fromisoformat(from_datetime),
        )

    def gzip_csv(
        event: str, 
        date: str,
        **kwargs
    ) -> list:
        append_file_list = kwargs['ti'].xcom_pull(task_ids=f'list_event_csv_{event}')
        target_gz_file = list(set([
            hrs.split('/')[-1][:2]
            for hrs in append_file_list
        ]))
        
        target_file_exists = {
            hr: dict(
                exist=check_target_zip_existance(event, date, hr),
                subsets=[file for file in append_file_list[1:] if file.split('/')[-1][:2] == hr]
            )
            for hr in target_gz_file
        }

        zip_keys = []
        for hour, target_dict in target_file_exists.items():
            if target_dict['exist']:

                zip_key = s3_hook.download_file(
                    bucket_name=BUCKET,
                    key=f'target/{event}/{date}/{hour}.csv.gz',
                    preserve_file_name=True,
                )
            else:
                zip_key = f'{hour}.csv.gz'

            with gzip.open(zip_key, 'wb') as gz_file:
                for file_key in target_dict['subsets']:
                    file_data = s3_hook.read_key(
                        file_key,
                        BUCKET, 
                    )
                    gz_file.write(file_data.encode())

            zip_keys.append(zip_key)

        return zip_keys


    def upload_files_to_s3(
        event: str,
        date: str, 
        **kwargs
    ) -> None:

        upload_file_list = kwargs['ti'].xcom_pull(task_ids=f'zip_data_{event}')
        
        for gzfile in upload_file_list:
            zip_file = gzfile.split('/')[-1]
            s3_hook.load_file(
                filename=gzfile,
                key=f'target/{event}/{date}/{zip_file}',
                bucket_name=BUCKET,
                replace=True,
            )

            print(f"Zip file '{gzfile}' created and uploaded to S3.")

    event_list = get_event_list()

    for event in event_list:

        get_new_csv_list = PythonOperator(
            task_id=f'list_event_csv_{event}',
            python_callable=get_keys_from_prefix,
            op_kwargs=dict(
                event=event,
                event_date=BACTH_DATE,
                from_datetime='{{ data_interval_start }}',
            )
        )

        zip_data = PythonOperator(
            task_id=f'zip_data_{event}',
            python_callable=gzip_csv,
            op_kwargs=dict(
                event=event,
                date=BACTH_DATE,
            )
        )

        upload_data = PythonOperator(
            task_id=f'upload_data_{event}',
            python_callable=upload_files_to_s3,
            op_kwargs=dict(
                event=event,
                date=BACTH_DATE,
            )
        )

        get_new_csv_list >> zip_data >> upload_data
