
# Analytics Engineering 


# Dbt
dbt stands for data build tool. It's a transformation tool that allows to process raw data in our Data Warehouse to transformed data which can be later used by Business Intelligence tools and any other data consumers.

dbt also allows us to introduce good software engineering practices by defining a deployment workflow:
    * Develop models
    * Test and document models
    * Deploy models with version control and CI/CD.

# Dbt Folder Structure 
1. `dbt_project.yml` - Tells dbt where to look for models
2. `packages.yml` - List of packages to be installed. Run command `dpt deps` and packages specified in package.yml file gets installed
3. `models` - Folder structure in which dbt sql logic and source data sets live here. It branches into staging, core folders
     * `staging` - Contains the raw models
     * `core` - Contains the models that we will expose at the end to the BI tool, stakeholders, etc.
4. All these models are going to be materialized in snowflake
5. `marts` - materialize all the models in marts folder as tables
8. `seeds` - seeds contain static files where data doesn't change very often
9. `snapshots` - useful when trying to create incremental models
10. `tests` - singluar test and generic test - Parameterized queries that accept arguments Eg: Chceck if this models has values > 0
11. `macros` - Macros allow us to add features to SQL that aren't otherwise available. Can develop/write reusable custom macros 

