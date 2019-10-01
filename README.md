# pipeline_USNVC
externalized usnvc logic for consumption by the bis data pipeline. 

## Structure 
- A pipeline processing script should have an entry file which we will infer the pipeline name from.
- It can implement any number of process methods.
- They should be named process_1, process_2, {...}, process_nn ect.
- Each process method has the exact same signature described below.
- Each processing method should return an integer equal to the number of rows it manipulates. 


## Process Method Signature
### Inputs
- path: The location of source data requested by this pipeline
- file_name: The name of the source data file or directory
- ch_ledger: An instance of the change leger class.
    - Example: ch_ledger.log_change_event("Field Creation", "Creating feature_id field from REG_NUM", source_data, changed_data)
- send_final_result: Instance of a method that accepts a python object representation of a single row of completed, processed data
- send_to_stage: Instance of a method that accepts a python object representation of a single row of data that will be processed by the next stage and the integer stage to send it to. 
- previous_stage_result: The python object provided by the previous stage.
### Outputs
- Returns an integer representing the number of rows manipulated

## Run Locally
uncomment the code surrounding the main() method