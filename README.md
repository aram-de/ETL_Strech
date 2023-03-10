<a name="readme-top"></a>

# **Café ETL Pipeline**
<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#project-background">Project Background</a>
      <ul>
        <li><a href="#design-choices">Design Choices</a></li>
        <li>
        <a href="#proof-of-concept">Proof of Concept</a>
        <li><a href="#moving-etl-to-cloud">Moving ETL to Cloud</a>
        </li>
      </ul>
    </li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

<!-- PROJECT BACKGROUND -->
## **Project Background**
This project was a team project part of the Generation UK & Ireland Data Engineering Bootcamp.

By the end of the project there were several stretch goals that we did not achieve, so my plan is to work on on the project to improve my skills and achieve those stretch goals.

The idea is to create an automate ETL pipeline for a caffe franchine that generates a daily CSV with their sales data for each franchise and stores in a local database.

The pipeline is intented to be a convenient method of collating and querying data from all outlets. So that they can make data informed decisions about attracting customers and identify sales trends across all outlets.

The targets for the project are:
-  **To develop an automated, fully scalable ETL (Extract, Transform, Load) pipeline** to handle the high volumes of data generated by the business and bring the data from the 100+ branches together into a central repository via use of a data warehouse
- **Make use of application monitoring software to produce operational metrics** such as how often the system is run, errors, up-time and more - this is useful for us as developers as it allows us to ensure our solution is functioning successfully and aid us in identifying bugs
- **Connect the data warehouse to analytics software** in order to create business intelligence analytics for the client. Some questions they would like answered include:
  - What is the best selling product across all stores in a given month?
  - What product is the most popular across all stores?
  - Which store is the most/least profitable?
  - When is the ‘rush hour’ of the business?
  - Which payment type is more popular by store?
  - Does price influence popularity (if at all?)
  - Are people mostly buying one item or multiple in a single transaction?

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### **Design Choices**
 In the project as we delivered we used:
  - An S3 bucket to collect the CSV files.
  - A lambda to transform the data and load it onto
  - Redshift
  - Cloudwatch for monitoring
  - Grafana for visualisation
  - Docker as a container for Grafana
  - An EC2 instance to run the Docker

My intention is to expand the project to achived the following (effectively my TODO).


  - Use of Cloudformation and Github to create a CI/CD pipeline
  - Use of SQS to create a queue
  - Use 2 lambdas, one to extract and transform and the second to do the loading.
  - Use a second S3 bucket to store transformed CSV files that can then be copied to Redshit (much faster than inserting from dataframes)

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### **Data Normalisation and Schema**
In the group project, we first needed took a look at a CSV from the company to decide on what tables we needed. The data was normalised to Third Normal Form and checked with our project owner/technical lead. Through the normalisation process, we also identified relationships that would need to exist in order for the data to be queryable.

**Resulting tables:**
##### **Note:** asterisk(*) = primary key; *italics* = foreign key


- **Transactions**
  | transaction_id* | timestamp | *store_id* | total_price | *payment_method_id*
  | ----------- | ----------- |----------- | ----------- |----------- |
  | 1 | 25/08/2021 09:00:00 | 1 |  2.45| 1 |
  | 1 | 25/08/2021 09:02:00 | 1 |  7.95| 1 |
- **Payment_method**
  | payment_method_id* | payment_method |
  | ----------- | ----------- |
  | 1 | CARD |
  | 2 | CASH |
- **Stores**
  | store_id* | store_name |
  | ----------- | ----------- |
  | 1 | Chesterfield |
  | 2 | Uppingham |
- **Products**
  | product_id* | product_name | price |
  | ----------- | ----------- | ----------- |
  | 1 | Regular Latte | 2.45|
  | 2 | Large Flavoured Latte - Hazelnut | 2.75|
- **Sales**
  | sales_id* | *transaction_id* | *product_id* |
  | ----------- | ----------- | ----------- |
  | 1 | 1 | 1|
  | 2 | 2 | 1 |
  | 2 | 2 | 1 |

We discussed breaking down the products table further, into separate size, type and flavour columns, however after considering the impact such a breakdown could have on join times which could impact querying the data, we ultimately decided to stick with a single table for products.

For a full breakdown of the normalisation process, please see <a href="https://github.com/DELON8/group-5-data-engineering-final-project/blob/main/supplementary_documentation/data_normalisation.pdf">here for documentation</a>.

With the data normalised, we were then able to design our schema.
![Final Schema](https://user-images.githubusercontent.com/116800613/213326064-a9672af7-8e2a-4011-b455-18baea46e145.png)


<p align="right">(<a href="#readme-top">back to top</a>)</p>

### **Data Extraction**
![DataInChecks drawio](https://user-images.githubusercontent.com/116560975/207384408-c7846e88-62be-4846-9258-e5805449943e.png)

Although the CSV file we used for our POC had no headers, we thought about other cases that we could encounter that could potentially crash our app. For example:

- We may receive a file with missing columns.
- We may receive a file with or without headers.
- We may receive a file with incorrect headers.

After careful consideration, we came up with the following:
- If the number of columns the extraction fails and the script outputs an error.
- If the number of columns is right but there are no headers. The script outputs a warning, adds the headers and extracts the data and makes it available as a pandas dataframe.
- If the headers are present and correct, just extract the data and present it as a pandas dataframe.

Furthermore, seven unit tests have been developed to help minimize regression.

![image](https://user-images.githubusercontent.com/116560975/207386669-ed25ddb8-a9fe-4392-bb75-8558d8e84a56.png)

## **Contributors**

<p><a href="https://github.com/Numan-M/
">https://github.com/Numan-M</a></p>

<p><a href="https://github.com/NylyxO/
">https://github.com/NylyxO/</a></p>



## **Contact**
<p><a href="https://www.linkedin.com/in/aram-morera-mesa/
">https://www.linkedin.com/in/aram-morera-mesa/</a></p>


<p align="right">(<a href="#readme-top">back to top</a>)</p>

## **Acknowledgments**

Thanks to Asif B., Numan, Maryan and Nyl who were my partners in the original group project and are amazing people to work with.

Thanks to Patrick, Saira, Megha and Zack for their support during the bootcamp.

<p align="right">(<a href="#readme-top">back to top</a>)</p>
