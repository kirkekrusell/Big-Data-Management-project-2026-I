
# Project 4 README

In order to follow our steps, do the following:
1. clone the repositary
`git clone <repo-url>`
2. open Docker Desktop and Terminal
In terminal: 
- `cd ...` to the project folder
- `make up`
- `make pull-models` - this step may take some time
Access the DAG and trigger runs.
3. Open http://localhost:8080/
4. Trigger the DAG
Run the pipeline with a LIMIT parameter.
    Click the DAG → Trigger DAG
    Set LIMIT=5 for development
    Watch tasks execute in the Graph view
<img width="1353" height="434" alt="image" src="https://github.com/user-attachments/assets/fbe539ab-2299-4bbf-91db-3534f592f969" />
5. Shut everything down
Afterwards `make clean` and all the conteiners are closed and cachew deleted (docker compose down -v)

