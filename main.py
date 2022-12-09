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
                                film_id SMALLINT UNSIGNED NOT NULL,
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

    cursor.execute("SELECT first_name FROM reviewer WHERE reviewer_id = ?", (reviewer_id,))
    reviewer_first_name = cursor.fetchone()
    cursor.execute("SELECT last_name FROM reviewer WHERE reviewer_id = ?", (reviewer_id,))
    reviewer_last_name = cursor.fetchone()

    # Ask for a film name to rate.
    # Greeting the reviewer with a greeting message.
    print(f"Hello, {reviewer_first_name[0]} {reviewer_last_name[0]}")

    # Ask to enter a film title.
    while True:
        film_name = input("Please Enter the film name you want to rate: ")
        # Go to the 'film' table and check if its title appears more than once.
        # If film_title doesn't exist, it will return an empty set.
        # If there are multiple films with the same name, it will return the title with its multiple IDs.
        # Else, the title appears only once, the query will return it.

        cursor.execute("SELECT title FROM film WHERE title = ?", (film_name,))
        film_title = cursor.fetchall()
        # If film_title doesn't exist
        if not film_title:
            print("This film name doesn't exist.")
        # If there are multiple films with the same name
        elif len(film_title) > 1:
            cursor.execute("SELECT film_id, release_year FROM film WHERE title = ?", (film_name,))
            film_id_year = cursor.fetchall()
        # Printing the multiple film ID's and their release year.
            for r in film_id_year:
                print(f"Film ID: {r[0]} , Release year: {r[1]}")

            film_id = input("Please Enter the film ID you wish to rate: ")
            cursor.execute("SELECT film_id FROM film WHERE film_id = ?", (film_id,))

            film_id_correct = cursor.fetchone()
            if film_id_correct is None:
                continue
            else:
                # Request a rating.
                rating(film_name, reviewer_id, film_id_correct)
                break
        # The title appears only once
        else:
            # Request a rating.
            rating(film_name, reviewer_id, None)
            break

    # Print all ratings.
    print_ratings()


# Request a rating.
def rating(film_name, reviewer_id, film_id_correct=None):
    while True:
        if film_id_correct is None:
            cursor.execute("SELECT film_id FROM film WHERE title = ?", (film_name,))
            film_id_correct = cursor.fetchone()
        film_rating = input(f"Enter a rating for the film {film_name}:")
        try:
            # Start transaction
            cnx.start_transaction()
            cursor.execute("INSERT INTO rating (film_id, reviewer_id, rating) VALUES (?, ?, ?) "
                           "ON DUPLICATE KEY UPDATE rating = VALUES (rating)",
                           (film_id_correct[0], reviewer_id, film_rating))
            result = cursor.fetchall()
            break
        except:
            # Rollback in case there is an error.
            cnx.rollback()
            print("You entered an invalid rating, please try again.")


def print_ratings():

    cursor.execute("SELECT f.title, CONCAT(rev.first_name, ' ', rev.last_name) AS reviewer_full_name, rat.rating "
                   "FROM rating AS rat, film AS f, reviewer AS rev "
                   "WHERE rat.film_id = f.film_id AND rat.reviewer_id = rev.reviewer_id "
                   "LIMIT 100")
    output = cursor.fetchall()
    for r in output:
        print(f"Film title: {r[0]}, Reviewer‘s full name: {r[1]}, Rating: {r[2]}")


if __name__ == '__main__':
    manage()
