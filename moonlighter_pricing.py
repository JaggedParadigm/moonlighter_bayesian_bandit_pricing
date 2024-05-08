from functools import reduce
import sqlite3
import pandas as pd

def enforce_foreign_key_constraints(cursor):
    cursor.execute("pragma foreign_keys = on;")
def get_moonlighter_data_connection():
    return sqlite3.connect("moonlighter_db.sqlite")
def transact_w_database(f):
    con = get_moonlighter_data_connection()
    cursor = con.cursor()
    try:
        enforce_foreign_key_constraints(cursor)
        result = f(con=con, cursor=cursor)
        error = None
    except Exception as e:
        result = None
        error = e
    finally:
        con.commit()
        con.close()
    if error is not None:
        raise Exception(error)
    return result
def query_data(query, params={}):
    return (
        transact_w_database(
            lambda con, cursor: (
                pd.read_sql(
                    query,
                    con=con,
                    params=params))))
def query_data_without_output(query, params={}):
    return (
        transact_w_database(
            lambda con, cursor: (
                cursor.execute(
                    query,
                    params))))
def initialize_shelves(shelf_count):
    return (
        transact_w_database(
            lambda con, cursor: (
                cursor.executemany(
                    f"""
                    insert or ignore into shelves(id, item, price) values(?, null, null)
                    """,
                    [[x] for x in range(shelf_count)]))))
def execute_sqlite_script(query):
    return (
        transact_w_database(
            lambda con, cursor: (
                cursor.executescript(
                    query))))
def drop_table(table_name):
    query_data_without_output(f"""drop table if exists {table_name}""")
def move_allowed_tables_to_end(table_names):
    return (
        [
            table_name
            for table_name in table_names
            if 'allowed_' not in table_name]
        + [
            table_name
            for table_name in table_names
            if 'allowed_' in table_name])
def get_current_tables():
    return (
        pipe(
            query_data("""
                select distinct
                    tbl_name
                from sqlite_master
                where
                    type = 'table'"""),
            lambda query_result: (
                [] if query_result is None else
                (
                    query_result
                    ['tbl_name']
                    .pipe(list)))))
def clear_database():
    for table_name in move_allowed_tables_to_end(get_current_tables()):
        drop_table(table_name)
def initialize_database():
    execute_sqlite_script(
        """
        pragma foreign_keys = on;

        /* Bounds used to simulate customer reactions to prices */
        create table if not exists price_reaction_bounds (
            item text not null,
            cheap_upper integer not null,
            perfect_upper integer not null,
            expensive_upper integer not null
        ) strict;

        create table if not exists allowed_moods (
            mood text primary key
        ) strict;
        insert or ignore into allowed_moods(mood) values
            ('ecstatic'),
            ('content'),
            ('sad'),
            ('angry');

        create table if not exists reactions (
            shelf_id integer not null,
            item text not null,
            price integer not null,
            mood text not null,
            foreign key (mood) references allowed_moods(mood)
        ) strict;
        create table if not exists price_bound_history (
            reaction_id integer,
            item text not null,
            low integer not null,
            high integer not null
        ) strict;
        create table if not exists inventory (
            item text not null,
            count integer not null
        ) strict;
        create table if not exists inventory_changes (
            item text not null,
            change integer not null
        ) strict;
        create table if not exists shelves (
            id integer not null,
            item text,
            price integer
        ) strict;
        create table if not exists shelf_history (
            shelf_id integer not null,
            item text,
            price integer
        ) strict;
        """)
def pipe(data, *functions):
    return reduce(lambda a, x: x(a), functions, data)
def execute_many_queries(query, data):
    return (
        transact_w_database(
            lambda con, cursor: (
                cursor.executemany(
                    query,
                    data))))
def bulk_update_inventory_changes(item_changes):
    execute_many_queries(
        query=f"""
            insert into inventory_changes values(
                :item,
                :change)""",
        data=[
            {
                'item': item,
                'change': change}
            for item, change in item_changes.items()])
def update_inventory():
    execute_sqlite_script(
        """
        pragma foreign_keys = on;

        drop table if exists inventory;

        create table inventory as
            select
                item,
                sum(change) as count
            from inventory_changes
            group by
                item
            having
                count >= 0;
        """)
def add_items_2_inventory(item_counts: dict):
    bulk_update_inventory_changes(
        item_changes=item_counts)
    update_inventory()


if __name__ == '__main__':
    clear_database()
    initialize_database()
    initialize_shelves(shelf_count=4)
    item_count = 20
    add_items_2_inventory(
        item_counts={
            'gold_runes': item_count,
            'broken_sword': item_count,
            'vine': item_count,
            'root': item_count,
            'hardened_steel': item_count,
            'glass_lenses': item_count,
            'teeth_stone': item_count,
            'iron_bar': item_count,
            'crystallized_energy': item_count,
            'golem_core': item_count})
    print(
        query_data(
            """
            select * from inventory_changes
            """
        )
    )
