# Crowd Task Manager
This system serves as a Crowdsourcing Task managing system, preparing data and assigning available tasks to the appropriate users in crowdsourcing scenarios.

## Description
In its current state, the system is optimized for a crowd-assisted Optical Music Recognition (OMR) pipeline. More specifically, the functionalities of the system can be summarized as follows:
1. The system receives PDF files of music scores
2. A measure detector identifies measures per page and creates a "skeleton" MEI file for each score
3. Each PDF file is segmented and associated with parts of the generated MEI file
4. Each segment and MEI parts are paired and served in crowdsourcing tasks
5. The system identifies available crowdsourcing interfaces and serves tasks to the crowd
6. Crowd judgements are processed and aggregated
7. Crowd-processed MEI parts are aggregated to re-create original PDF submitted music score
8. Versions of the crowd-processed MEI file are pushed to Github periodically until the end of the transcription campaign

## Roadmap
The following functionalities are currently under development:
* __Task Scheduling__: Create crowdsourcing tasks dynamically based on the data needs and results from the crowd
* __Task Assignment__: Assign crowdsourcing tasks to users based on user's profile. Coming up soon:
	* Prevent users receiving the same task
	* Assign tasks based on music instrument of choice (for Orchestra members)
* __MEI improvement pipeline__: If an MEI file exists for a given PDF music score, the pipeline will be able to adapt and improve the quality of the provided MEI file.

## Major Dependencies
- CE-API ([Github page](https://github.com/trompamusic/ce-api))
- Measure Detector ([Github page](https://github.com/OMR-Research/MeasureDetector/))
- Python modules (see Setup)
- MongoDB ([how to install](https://docs.mongodb.com/manual/installation/))
- RabbitMQ ([how to install](https://www.rabbitmq.com/download.html))

## Setup
2. Install all dependencies
2. Use `pip3 install -r requirements.txt` to install all the relevant python modules
3. Use the settings_example.yaml to create a settings.yaml. Fill in the settings.yaml with  location of:
	- MongoDB
	- RabbitMQ
	- GitHub account
	- Server name/address
4. Use `sh run_modules.sh` script to start/restart the modules (use `chmod +x run_modules.sh` if you lack permissions to run the script)
5. Use `sh stop_modules.sh` script to stop the modules (use `chmod +x stop_modules.sh` if you lack permissions to run the script)
