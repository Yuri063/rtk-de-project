
**RTK-DE-PROJECT: Итоговая работа по курсу РТК Data Engineer**
---
***

***Folder: dashboards***
***
    ReportBilling_Project_DWH.pbix      - отчет (dashboard) PowerBI (в формате pbix)
    
    ReportBilling_Project_DWH.png       - printscreen отчета PowerBI (в формате png)

***Folder: DDL***
***
    dwh_project_ddl&test.sql            - SQL-скрипты для DDL-операций и тестов ETL-процедур для DWH

***Folder: ERD***
***
    ERD_DDS_Project_DWH.png             - ER-диаграмма детального слоя DWH (в формате png)
    
    ERD_DM_Project_DWH.png              - ER-диаграмма слоя витрин данных DWH (в формате png)
    
    ERD_STG_ODS_DDS_Project_DWH.drawio  - ER-диаграмма слоев DWH: staging, операциооный слой, детальный слой 
                                          (в формате drawio)
                                          
    ERD_STG_ODS_DDS_Project_DWH.pdf     - ER-диаграмма слоев DWH: staging, операциооный слой, детальный слой 
                                          (в формате pdf)
                                          
    ERD_STG_ODS_DDS_Project_DWH.png     - ER-диаграмма слоев DWH: staging, операциооный слой, детальный слой 
                                          (в формате png)

***Folder: great_expectations*** 
***
                                        - Проверки качества для наборов данных с использованием Python-based
                                          open-source library Great Expectations
                                          Полный отчет по проверкам - см. файл data_docs.zip
 
***Folder: SQL*** 
***
                                        - SQL-скрипты для ETL-процедур DWH: hubs, links, satellites, facts, 
                                          dimensions, etc.

***Files:*** 
***
    dwh_project.py                      - Python-скрипт для DAG-Airflow, содержащий сценарий оркестрации 
                                          ETL-поцедур для DWH
    
