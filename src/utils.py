import psycopg2
from src.classes import HeadHunterAPI, DBManager
import requests
from src.config import config
from pprint import pprint


EMPLOYERS = ['Ростелеком', 'Сбер', 'Касперский', 'Ростех',
             'Айтеко', '1С', 'OCS', 'МОНТ', 'Яндекс', 'Газпром']


def main():
    params = config()
    db_name = 'hh_vacancies'
    data = get_data(EMPLOYERS)
    create_database(db_name, params)
    save_data_to_database(data, db_name, params)
    database = DBManager(db_name, params)
    # Интерфейс взаимодействия с пользователем
    while True:
        user_answer = int(input('Введите команду:\n'
                                '1 - Получить список всех кампаний\n'
                                '2 - Получить список всех вакансий\n'
                                '3 - Получить среднюю з/п по вакансиям\n'
                                '4 - Получить список вакансий с з/п выше средней\n'
                                '5 - Найти вакансии по ключевому слову\n'
                                '6 - Выйти\n'))
        if user_answer == 1:
            response = database.get_companies_and_vacancies_count()
            pprint(response)
        elif user_answer == 2:
            response = database.get_all_vacancies()
            pprint(response)
        elif user_answer == 3:
            response = database.get_avg_salary()
            pprint(response)
        elif user_answer == 4:
            response = database.get_vacancies_with_higher_salary()
            pprint(response)
        elif user_answer == 5:
            keyword = input('Введите ключевое слово\n')
            response = database.get_vacancies_with_keyword(keyword)
            pprint(response)
        elif user_answer == 6:
            break
        else:
            print('Неизвестная команда попробуйте снова')


def get_data(emloyers_list):
    """Получает данные по работодателям и вакансиям"""
    vacancies_data = []
    for item in emloyers_list:
        hh = HeadHunterAPI(item)
        employers = hh.get_employers()
        vacancies_response = requests.get(employers[0]['vacancies_url']).json()
        employers_response = requests.get(employers[0]['url']).json()

        for employer in employers:
            if employer['name'].lower() == item.lower():
                vacancies_response = requests.get(employer['vacancies_url']).json()
                employers_response = requests.get(employer['url']).json()
                break

        vacancies_data.append({
            'employer': {
                'id': employers_response['id'],
                'name': employers_response['name'],
                'open_vacancies': employers_response['open_vacancies'],
                'url': employers_response['alternate_url'],
                'site_url': employers_response['site_url']
            },
            'vacancies': vacancies_response['items']
        })
    return vacancies_data


def create_database(db_name, params):
    """Создаёт новую базу данных"""
    try:
        conn = psycopg2.connect(database='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()
        # Удаляем базу, если уже есть одноимённая
        cur.execute(f"SELECT pg_terminate_backend(pg_stat_activity.pid) "
                    f"FROM pg_stat_activity "
                    f"WHERE pg_stat_activity.datname = '{db_name}' "
                    f"AND pid <> pg_backend_pid()")
        cur.execute("DROP DATABASE " + db_name)
        cur.execute("CREATE DATABASE " + db_name)
        cur.close()
        conn.close()
    except psycopg2.errors.InvalidCatalogName:
        # Если базы данных с таким именем не существует
        cur.execute("CREATE DATABASE " + db_name)
        cur.close()
        conn.close()
    except psycopg2.errors.Error as e:
        raise e

    connect = psycopg2.connect(database=db_name, **params)
    with connect as conn:
        with conn.cursor() as cur:
            cur.execute(
                'CREATE TABLE employers ('
                'employer_id int PRIMARY KEY, '
                'employer_name varchar(100), '
                'open_vacancies int, '
                'url varchar(100), '
                'site_url varchar(100)'
                ')'
            )
            cur.execute(
                'CREATE TABLE vacancies ('
                'vacancy_id int PRIMARY KEY, '
                'vacancy_name varchar(100), '
                'city varchar(50), '
                'salary_from int, '
                'salary_to int, '
                'salary_currency char(3), '
                'requirements text, '
                'url varchar(100), '
                'employer_id int NOT NULL,'
                'FOREIGN KEY (employer_id) REFERENCES employers(employer_id)'
                ')'
            )
    conn.close()


def save_data_to_database(data, db_name, params):
    """Сохраняет полученную информацию в базу данных"""
    connect = psycopg2.connect(database=db_name, **params)
    with connect as conn:
        with conn.cursor() as cur:
            for employer in data:
                # Заполняем таблицу employers
                employer_id = employer['employer']['id']
                employer_name = employer['employer']['name']
                open_vacancies = employer['employer']['open_vacancies']
                employer_url = employer['employer']['url']
                site_url = employer['employer']['site_url']
                cur.execute(
                    'INSERT INTO employers '
                    'VALUES (%s, %s, %s, %s, %s)',
                    (employer_id, employer_name, open_vacancies, employer_url, site_url)
                )
                # Заполняем таблицу vacancies
                for vacancy in employer['vacancies']:
                    try:
                        vacancy_id = vacancy['id']
                        vacancy_name = vacancy['name']
                        city = vacancy['area']['name']
                        salary_from = vacancy['salary']['from']
                        salary_to = vacancy['salary']['to']
                        salary_currency = vacancy['salary']['currency']
                        requirements = vacancy['snippet']['requirement']
                        url = vacancy['alternate_url']
                    except TypeError:
                        vacancy_id = vacancy['id']
                        vacancy_name = vacancy['name']
                        city = vacancy['area']['name']
                        salary_from = None
                        salary_to = None
                        salary_currency = None
                        requirements = vacancy['snippet']['requirement']
                        url = vacancy['alternate_url']

                    cur.execute(
                        'INSERT INTO vacancies '
                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                        (vacancy_id, vacancy_name, city, salary_from, salary_to,
                         salary_currency, requirements, url, employer_id)
                    )
    conn.close()
