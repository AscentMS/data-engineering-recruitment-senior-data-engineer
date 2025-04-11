Testing the fork

# Senior Data Engineer Interview Task 
At Evergreen we deal with disparate sources of sensitive customer and geographical data across various departments, with examples ranging over structured, to semi-structured, to unstructured data. Our objective is to centralise these data sources into our relational data warehouse to extract valuable insights and facilitate data-driven decisions, whilst respecting the uniquely sensitive nature of health data.

The task below touches on several different areas of how data engineering operates at Evergreen, spanning data modelling, integration, deployment, etc. We ask that you try to complete as much as you can within **120 minutes**. The task methodology is purposefully open to allow us to observe your approach to the problem: our only prerequisite is that you use Python as your programming language, and demonstrate some flavour of SQL somewhere in the solution.

## Task
Your task is to develop a data pipeline that retrieves and integrates two datasets into an analytics database to query for specific information. Write a Python script which retrieves the two files below, and loads them into a local relational database (an in-memory / file base database is fine). Then, using your database, write a SQL query that demonstrates how many distinct users there are by local authority. 

- The Customer Data file should be anonymised prior to being used for analytics.
- Account for potential changes in the data, such as when a user moves home.
- The data must be formatted such that it allows a data scientist/analyst to easily determine how many users there are in any given local authority

Create a branch in this repository to hold your solution, and commit all your changes to the branch. When you're finished, submit a Pull Request; we will then review the solution in the follow-up interview. Feel free to add comments both in the code itself, and in the Pull Request, to clarify on any points you feel necessary.

To make it easier for others to run your code and demonstrate code deployment principles,  please add your solution to a container image.

Do not commit the processed data / credentials / etc. to source control; we expect you to demonstrate the process executing.
## Data Sources
- [Postcode Data](https://evergreen-life-interview.s3.eu-west-2.amazonaws.com/postcodes.zip)
- [Customer Data](https://evergreen-life-interview.s3.eu-west-2.amazonaws.com/users.json)

## Bonus Considerations 
- Package your code as a python package.
- Show how your code could be utilised in an Airflow DAG.
- Demonstrate other analyses / metrics that could be retrieved from these datasets.
- Add tests to your package.
- Demonstrate good database practices as appropriate to the technology (e.g. indexing, row vs columnular orientation, etc.)
- Demonstrate data / software engineering principles: e.g. Idempotency, SOLID design, etc.
