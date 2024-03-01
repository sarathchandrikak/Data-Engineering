# Dbt


# 
1. dbt_project.yml - Tells dbt where to look for models
2. dbt_packages - by running dpt deps packages specified in package.yml file gets installed
3. models -where we will be runnning sql logic and source data sets live here
4. Separate staging and source files and marts folders
5. All these models are going to be materialized in snowflake
6. staging - materialize all the models in staging folder as views
7. marts - materialize all the models in marts folder as tables
8. seeds - seeds contain static files where data doesn't change very often
9. snapshots - useful when trying to create incremental models
10. tests - singluar test and generic test - Parameterized queries that accept arguments Eg: Chceck if this models has values > 0
11. 
12. macros - can write reusable macros 

