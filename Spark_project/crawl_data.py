import requests
from bs4 import BeautifulSoup
import csv
from pyspark.sql import SparkSession
import findspark
findspark.init()

headers = ({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36", 'Accept-Language': 'en-US, en;q=0.5'
    })
url = "https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&searchTextSrc=&searchTextText=&txtKeywords=C%23+developer&txtLocation="

def crawl_1object():
    reponse = requests.get(url, headers)
    soup = BeautifulSoup(reponse.content, "html.parser")
    # get object
    desired_tag = soup.find("li", class_ = "clearfix job-bx wht-shd-bx")
    # get job name
    job_name = desired_tag.find("a", attrs= {"target": "_blank"})
    print(job_name.text.strip())
    # get company name
    comp_name = soup.find("h3", class_ = "joblist-comp-name")
    print(comp_name.text.strip())
    # get job description
    tmp = soup.find("ul", class_ = "list-job-dtl clearfix")
    tmp1 = tmp.find("li")
    tmp2 = tmp1.text.split()
    job_des, skills = "", ""
    for i in range(4, len(tmp2)): job_des += tmp2[i] + " "
    print(job_des)
    # get key skills
    tmp1 = tmp.find("span", class_ = "srp-skills")
    for i in tmp1.text.strip().split():
        if i != "," and i != " ": skills += i + ", "
    print(skills)



def crawl_all_data():
    with open("C#_developer_job.csv", 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Job name", "Company Name", "Job description", "Key skills"])
    for i in range(1, 201):
        url = f"https://www.timesjobs.com/candidate/job-search.html?from=submit&luceneResultSize=25&txtKeywords=c0HASH0%20developer&postWeek=60&searchType=personalizedSearch&actualTxtKeywords=C0HASH0%20developer&searchBy=0&rdoOperator=OR&pDate=I&sequence={i}&startPage={i}"
        with open("C#_developer_job.csv", "a", newline="") as file:
            writer = csv.writer(file)
            lst_job, lst_comp, lst_descrip, lst_key = [], [], [], []
            try:
                response = requests.get(url, headers= headers)
                soup = BeautifulSoup(response.content, 'html.parser')
                desired_tag = soup.find_all('li', class_ = "clearfix job-bx wht-shd-bx")
                for i in range(25):
                    name = desired_tag[i].find("a", {"target": "_blank"})
                    lst_job.append(name.text.strip())
                company_name = soup.find_all("h3", class_ = "joblist-comp-name")
                for i in range(25): lst_comp.append(company_name[i].text.strip())
                detail = soup.find_all("ul", class_ = "list-job-dtl clearfix")
                for i in range(25):
                    tmp2 = detail[i].find_all("li")
                    tmp = detail[i].find("span", class_ = "srp-skills")
                    new_text = ""
                    for i in tmp.text.strip().split():
                        if i != "," and i != " ": new_text += i + ", "
                    lst_descrip.append(new_text)
                    new_text = ""
                    text = tmp2[1].text.strip()
                    text = text.split()
                    for i in range(1, len(text)):new_text += text[i] + " "
                    lst_key.append(new_text)
                for i in range(25): writer.writerow([lst_job[i], lst_comp[i], lst_descrip[i], lst_key[i]])
            except requests.exceptions.RequestException as e:
                print(f"Error getting the page: {e}")

def dupplicate_data():
    spark = SparkSession.builder.appName("crawl_data").getOrCreate()
    df1 = spark.read.csv("C#_developer_job.csv", header = True, inferSchema = True)
    df3 = df1.union(df1)
    df4 = df3.union(df3)
    df5 = df4.union(df4)
    df6 = df5.union(df5)
    df7 = df6.union(df6)
    df8 = df7.union(df7)
    df9 = df8.union(df8)
    df10 = df9.union(df8)
    df10 = df10.repartition(1)
    df10.write.csv("data.csv", header = True)




if __name__ == "__main__":
    crawl_all_data()