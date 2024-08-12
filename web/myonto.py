import os
import sys
import itertools
from jiwer import cer
import pyparsing
import rdflib
from flask import Flask, render_template, flash, request
from wtforms import Form, StringField, validators
from query_api import extract_fields
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, levenshtein, length, lower, regexp_replace

app = Flask(__name__)
app.config['SECRET_KEY'] = '5791628bb0b13ce0c676dfde280ba245'
global no_query
global anything
spark = SparkSession.builder.appName("search").getOrCreate()
csv_file = './misc/merge.csv'
df = spark.read.csv(csv_file, header=True, inferSchema=True)
df.createOrReplaceTempView("csv_table")


class ReusableForm(Form):
    query = StringField('Query:', validators=[validators.data_required()])
    @app.route("/", methods=['GET', 'POST'])
    def hello():
        global graph
        form = ReusableForm(request.form)
        if form.errors:
            print(form.errors)

        output = {}
        query = ''

        if request.method == 'POST':


            if(request.form['but1']=='AllBook'):
                no_query = True
                query = """
                SELECT book_title,author,publisher,year FROM csv_table
                """

            
            else:
                anything = request.form['query']
                no_query = False
                if anything == "":
                    data = [["", "", "", ""]]
                else:
                    result_json = extract_fields(anything)
                    book_title = result_json['title']
                    author = result_json['author']
                    publisher = result_json['publisher']
                    year = result_json['year']
                    isbn = result_json['isbn']
                    unknown = result_json['unknown']
                    query = """
                    SELECT book_title,author,publisher,year FROM csv_table 
                    """
                    if unknown:
                        data = [["", "", "", ""]]
                    else:
                        # if author:
                        #     if "where" in query:
                        #         query += f"and author='{author}' "
                        #     else:
                        #         query += f"where author='{author}' "
                        if author:
                            author = author.lower()
                            if "where" in query:
                                query += f"and levenshtein(LOWER(author), '{author}') / (length('{author}')) < 0.2 or LOWER(author) LIKE '%{author}%' "
                            else:
                                query += f"where levenshtein(LOWER(author), '{author}') / (length('{author}')) < 0.2 or LOWER(author) LIKE '%{author}%' "
                        if book_title:
                            book_title = book_title.lower()
                            if "where" in query:                            
                                query += f"and levenshtein(LOWER(book_title), '{book_title}') / (length('{book_title}')) < 0.2 or LOWER(book_title) LIKE '%{book_title}%' "
                            else:
                                query += f"where levenshtein(LOWER(book_title), '{book_title}') / (length('{book_title}')) < 0.2 or LOWER(book_title) LIKE '%{book_title}%' "
                        if publisher:
                            if "where" in query: 
                                query += f"and publisher='{publisher}' "
                            else:
                                query += f"where publisher='{publisher}' "
                        if year:
                            if "where" in query: 
                                query += f"and year='{year}' "
                            else:
                                query += f"where year='{year}' "
                        if isbn:
                            if "where" in query: 
                                query += f"and isbn='{isbn}' "
                            else:
                                query += f"where isbn='{isbn}' "
                        print(query)
                        result = spark.sql(query)
                        result_list = result.collect()
                        data = [list(row.asDict().values()) for row in result_list]
                        if len(data)>=100:
                            data = data[:100]

                        data.sort()
                        data = list(k for k,_ in itertools.groupby(data))

                    
            cols = ["Book", "Author", "Publisher", "Year"]
            if no_query is False:
                if data==[]:
                    data = [["", "", "", ""]]
                if len(data)>=100:
                    data = data[:100]

            else:
                result = spark.sql(query)
                result_list = result.collect()
                data = [list(row.asDict().values()) for row in result_list]
                if len(data)>=100:
                    data = data[:100]


            output = {'columns': cols,
                      'data': data}
            flash('Results ready!')


        return render_template('home.html', form=form, title="Book Search", output=output)


if __name__ == "__main__":

    app.run(port=9000,debug=True)
