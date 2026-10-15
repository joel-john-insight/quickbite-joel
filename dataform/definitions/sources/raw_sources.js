// Declare the four raw tables that the Airflow DAG loads, so other files can use ref("orders") etc.
// The dataset name comes from workflow_settings.yaml (vars.raw_dataset), not hard-coded here.
const rawDataset = dataform.projectConfig.vars.raw_dataset;

["restaurants", "customers", "orders", "order_items"].forEach(name => {
  declare({ schema: rawDataset, name: name });
});
