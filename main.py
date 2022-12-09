import mysql.connector
import os
from dotenv import load_dotenv


path = 'C:\\Users\\liadb\\PycharmProjects\\SQL_Task1_reviewer\\sql_pass.env'
load_dotenv(path)
password = os.getenv('MYSQL_ROOT_PASSWORD')
# Create a connection
cnx = mysql.connector.connect(
    user='root',
    password=password,
    host='127.0.0.1',
    database='sakila'
)
# Set autocommit to true (this is circumstantial, think twice if you want to use this)
cnx.autocommit = True
# Create a cursor with prepared=True, necessary for running prepared statements
cursor = cnx.cursor(prepared=True)


def creat_tables():
    # Create the 'reviewer' table only if it does not already exist.
    # No two reviewers can have the same ID. To implement this, we use the 'UNIQUE' operator.
    cursor.execute("""
                            CREATE TABLE IF NOT EXISTS reviewer (
                                reviewer_id int NOT NULL PRIMARY KEY UNIQUE,
                                first_name VARCHAR(45) NOT NULL,
                                last_name VARCHAR(45) NOT NULL
                            );
                        """)

    # Create the 'rating' table only if it does not already exist.
    # The highest possible rating is 9.9, and the lowest possible rating is 0.0 according to the instructions.
    #   For this we will add the constraint 'CHECK' to varify whether the rating is within the valid range.
    # A rating record rates a single film, uniquely identified by film_id.
    #   To enforce this, we will define a FOREIGN KEY as film_id.
    # A rating record is rated by a single reviewer, uniquely identified by reviewer_id.
    #   To enforce this, we will define a FOREIGN KEY as reviewer_id.
    # A reviewer can rate a film only once.
    #   This will be implemented by using the 'UNIQUE' operator.
    # Each pair of film id and reviewer id will be unique.
    # Rating cannot be null.
    #   This will be implemented by using the 'NOT NULL' operator in the 'rating' column.
    # If the film was already rated by the reviewer, then the previous review should be updated.
    #  To support updates of already existing reviews; (film_id, reviewer_id) (meaning conflict),
    #  we will switch the old rating with the new one.

    cursor.execute("""
                            CREATE TABLE IF NOT EXISTS rating (
                                film_id SMALLINT UNSIGNED,
                                reviewer_id int,
                                rating DECIMAL(2,1) NOT NULL,
                                CHECK (rating BETWEEN 0.0 AND 9.9),
                                UNIQUE (film_id, reviewer_id),
                                FOREIGN KEY (film_id) 
                                REFERENCES film (film_id) ON DELETE CASCADE ON UPDATE CASCADE,
                                FOREIGN KEY (reviewer_id) 
                                REFERENCES reviewer (reviewer_id) ON DELETE CASCADE ON UPDATE CASCADE
                            );
                        """)


def manage():
    #cursor.execute("DROP TABLE rating ")
    #cursor.execute("DROP TABLE reviewer ")

    # Creating a 'reviewer' table and a 'rating' table, only if they do not already exist.
    #creat_tables()
    #idd = 2001
    # Execute the query.
    #cursor.execute("DELETE FROM film WHERE film_id = ?", (idd,))
    #cnx.commit()

    # Ask the reviewer for their ID.
    reviewer_id = input("Please Enter your ID: ")

    # Check if the ID exists in the database.
    cursor.execute("SELECT * FROM reviewer WHERE reviewer_id = ?", (reviewer_id,))
    reviewer = cursor.fetchone()

    # If the ID does not exist, ask for the reviewer's name and save it to the database.
    if reviewer is None:
        first_name = input("Enter your first name: ")
        last_name = input("Enter your last name: ")
        cursor.execute("INSERT INTO reviewer (reviewer_id, first_name, last_name) VALUES (?, ?, ?)",
                       (reviewer_id, first_name, last_name))
        cnx.commit()

    print(reviewer)

    cursor.execute("SELECT first_name FROM reviewer WHERE reviewer_id = ?", (reviewer_id,))
    reviewer_first_name = cursor.fetchone()
    cursor.execute("SELECT last_name FROM reviewer WHERE reviewer_id = ?", (reviewer_id,))
    reviewer_last_name = cursor.fetchone()

    # Ask for a film name to rate.
    # Greeting the reviewer with a greeting message.
    print(f"Hello, {reviewer_first_name[0]} {reviewer_last_name[0]}")

    # add the same name with different id to the film table
    # Define the new movie details
    #title = "ACE GOLDFINGER"
    #id = 2003
    #language_id = 3

    # Construct the INSERT statement
    #query = f"INSERT INTO film (film_id, title, language_id) VALUES({id}, '{title}', {language_id})"
    # Execute the query
    #cursor.execute(query)
    #cnx.autocommit = True


    # Print all film titles.
    cursor.execute("SELECT title, film_id FROM film WHERE title REGEXP '$'")
    film_name = cursor.fetchall()
    cnx.commit()

    print(film_name)

    # Ask to enter a film title.
    flag = True
    while flag:
        film_name = input("Please Enter the film name you want to rate:")
        # Check if the film name is in the data.
        cursor.execute("SELECT title FROM film WHERE title = ?", (film_name,))
        film_title = cursor.fetchall()
        cnx.commit()

        print(film_title)
        if not film_title:
            print("This film title doesn't exist, you need to enter a different film name")
        # Go to the 'film' table and check if its title appears more than once:
        # If the title appears only once, the query will return None.
        # else, it will return the title with its multiple IDs.
        # Construct the SELECT query.
        #  = f"SELECT title, COUNT(*) AS count FROM film WHERE title = '{film_name}' GROUP BY title HAVING count > 1"
        # Execute the query
        # cursor.execute(query)
        # film_many_titles = cursor.fetchall()
        # if film_titles is not None meaning there are at least 2 film titles with the same name
        elif len(film_title) > 1:
            cursor.execute("SELECT film_id, release_year FROM film WHERE title = ?", (film_name,))
            cnx.commit()

            film_id_year = cursor.fetchall()
            print(f"There is the id's of the film you chose: {film_id_year}")
            film_id = input("Please Enter the film id you want to rate:")
            cursor.execute("SELECT film_id FROM film WHERE film_id = ?", (film_id,))
            cnx.commit()

            film_id_correct = cursor.fetchone()
            if film_id_correct is None:
                flag = True
            else:
                # Request a rating.
                rating(film_name, reviewer_id, film_id_correct)
                break
        else:
            print("you chose correct movie that had 1 title")
            # Request a rating.
            rating(film_name, reviewer_id, None)
            flag = False

    # Print all ratings.
    print_ratings()
    cnx.commit()


# Request a rating.
def rating(film_name, reviewer_id, film_id_correct=None):
    flag = True
    while flag:
        if film_id_correct is None:
            cursor.execute("SELECT film_id FROM film WHERE title = ?", (film_name,))
            film_id_correct = cursor.fetchone()
        film_rating = input(f"Enter rating for the film {film_name}:")
        try:
            # Start transaction
            cnx.start_transaction()
            cursor.execute("INSERT INTO rating (film_id, reviewer_id, rating) VALUES (?, ?, ?) "
                           "ON DUPLICATE KEY UPDATE rating = VALUES (rating)",
                           (film_id_correct[0], reviewer_id, film_rating))
            result = cursor.fetchall()
            cnx.commit()
            print(result)

            flag = False
        except:
            # Rollback in case there is an error.
            cnx.rollback()
            print("You entered invalid rating, please try again")
    cnx.commit()


def print_ratings():

    cursor.execute("SELECT f.title, CONCAT(rev.first_name, ' ', rev.last_name) AS reviewer_full_name, rat.rating "
                   "FROM rating AS rat, film AS f, reviewer AS rev "
                   "WHERE rat.film_id = f.film_id AND rat.reviewer_id = rev.reviewer_id "
                   "LIMIT 100")
    output = cursor.fetchall()
    for r in output:
        print(f"Film title: {r[0]}, Reviewer‘s full name: {r[1]}, Rating number: {r[2]}")


if __name__ == '__main__':
    manage()
