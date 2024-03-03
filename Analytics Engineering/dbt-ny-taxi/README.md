# Dbt NY Taxi Project 


# Objective 


# Dbt

# Dbt Folder Structure 
1. dbt_project.yml - Tells dbt where to look for models
2. packages.yml - List of packages to be installed. Run command `dpt deps` and packages specified in package.yml file gets installed
3. models - Folder structure in which dbt sql logic and source data sets live here. It branches into staging, core folders
     * staging - Materialize all the models in this folder as views
     * core -
4. All these models are going to be materialized in snowflake
5. marts - materialize all the models in marts folder as tables
8. seeds - seeds contain static files where data doesn't change very often
9. snapshots - useful when trying to create incremental models
10. tests - singluar test and generic test - Parameterized queries that accept arguments Eg: Chceck if this models has values > 0
11. macros - can write reusable macros 

