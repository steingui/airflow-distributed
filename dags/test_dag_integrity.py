import unittest
from airflow.models import DagBag

class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        # Load the DAG bag pointing to the dags folder
        self.dagbag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)

    def test_dag_imports_with_no_errors(self):
        """Verify that the DAG bag can import our DAG without errors."""
        self.assertEqual(
            len(self.dagbag.import_errors), 
            0, 
            f"DAG import errors: {self.dagbag.import_errors}"
        )

    def test_dag_structure(self):
        """Verify the tasks and structure of our specific pipeline."""
        dag_id = 'fakestore_data_pipeline'
        self.assertIn(dag_id, self.dagbag.dags)
        dag = self.dagbag.dags[dag_id]
        
        # Verify specific tasks exist
        expected_tasks = {'create_tables', 'ingest_users', 'ingest_products', 'ingest_carts', 'notify_success'}
        actual_tasks = set(dag.task_ids)
        self.assertEqual(actual_tasks, expected_tasks)
        
        # Verify dependencies
        create_tables_task = dag.get_task('create_tables')
        self.assertEqual(
            set(create_tables_task.downstream_task_ids), 
            {'ingest_users', 'ingest_products', 'ingest_carts'}
        )
        
        notify_success_task = dag.get_task('notify_success')
        self.assertEqual(
            set(notify_success_task.upstream_task_ids), 
            {'ingest_users', 'ingest_products', 'ingest_carts'}
        )

if __name__ == "__main__":
    unittest.main()
