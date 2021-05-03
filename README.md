# Crowd Task Manager
This system serves as a Crowdsourcing Task managing system, preparing data and assigning available tasks to the appropriate users in crowdsourcing scenarios.

## Description
In its current state, the system is optimized for a crowd-assisted Optical Music Recognition (OMR) pipeline. More specifically, the functionalities of the system can be summarized as follows:
1. The system receives PDF files of music scores
2. A measure detector identifies measures per page and creates a "skeleton" MEI file for each score
3. Each PDF file is segmented and associated with parts of the generated MEI file
4. Each segment and MEI parts are paired and served in crowdsourcing tasks
5. The system identifies available crowdsourcing task types and makes tasks available via its API
6. As results are posted back to the API, crowd judgements are processed and aggregated
7. Crowd-processed MEI parts are aggregated to re-create original PDF submitted music score
8. Versions of the crowd-processed MEI file are pushed to Github (if enabled) periodically until the end of the transcription campaign

## Major Dependencies
- CE-API (optional) ([Github page](https://github.com/trompamusic/ce-api))
- Docker ([Website](https://www.docker.com/))

## Setup
1. If you plan to also run the front-end [scriptoria](https://github.com/cakefm/scriptoria/tree/refactor/components), clone it into a `scriptoria` folder in the root of this repository
2. Run `start_local.sh`, it will set up and start the docker containers automatically

## Future Work
There's a lot to still be done. To make this a little bit easier, issues have been made tagged with Future Work labels, in case anyone wants to carry on the torch. A couple of these might involve large refactors.
