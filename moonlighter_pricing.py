from functools import reduce
import sqlite3
import pandas as pd
import numpy as np

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
        create table if not exists thompson_competitions (
            competition_ind integer not null,
            item text not null,
            price_lower_bound integer not null,
            price_upper_bound integer not null,
            sampled_price integer not null
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
def use_map(f):
    def use_map_inner(x):
        return [f(y) for y in x]
    return use_map_inner
def initialize_price_bound_history(price_bounds):
    price_item_data = (
        pipe(
            [
                pipe(
                    price_bound.split('|'),
                    use_map(int),
                    lambda bounds: {
                        'low': bounds[0],
                        'high': bounds[1]},
                    lambda bound_data: [
                        {'item': item} | bound_data
                        for item in items])
                for price_bound, items in price_bounds.items()],
            lambda separated_price_item_data: (
                reduce(
                    lambda a, x: a + x,
                    separated_price_item_data))))
    execute_many_queries(
        query="""
            insert into price_bound_history values(
                null,
                :item,
                :low,
                :high)""",
        data=price_item_data)
def add_price_reaction_bounds(price_reaction_bounds):
    execute_many_queries(
        """
        insert into price_reaction_bounds values(
            :item,
            :cheap_upper,
            :perfect_upper,
            :expensive_upper)
        """,
        price_reaction_bounds)
def get_empty_shelf_ids():
    inventory_item_count = get_inventory_item_count()
    empty_shelf_ids = (
        query_data("""
            select distinct
                id
            from shelves
            where
                item is null""")
        ['id']
        .pipe(lambda x: [int(y) for y in x.values]))
    return (
        empty_shelf_ids if inventory_item_count >= len(empty_shelf_ids) else
        # Use only the number of empty shelves for which there are inventory
        # items to fill them with
        empty_shelf_ids[:inventory_item_count])
def add_thompson_competition(competition_data):
    execute_many_queries(
        """
        insert into thompson_competitions values(
            :competition_ind,
            :item,
            :price_lower_bound,
            :price_upper_bound,
            :sampled_price)
        """,
        competition_data)
def get_next_competition_index():
    return (
        query_data("""
            select
                max(competition_ind) as max_competition_ind
            from thompson_competitions
        """)
        ['max_competition_ind']
        .pipe(lambda x: (
            int(x.values[0]) + 1 if x.values[0] is not None else
            0)))
def get_thompson_competition(price_bounds, rng, next_competition_index):
    return (
        price_bounds
        .assign(**{
            'sampled_price': lambda y: (
                rng
                .integers(
                    y['low'],
                    y['high'] + 1)),
            'competition_ind': next_competition_index})
        .rename(
            columns={
                'low': 'price_lower_bound',
                'high': 'price_upper_bound'})
        [['competition_ind', 'item', 'price_lower_bound', 'price_upper_bound', 'sampled_price']])
def get_thompson_sampled_item_and_price(competition_data, rng):
    return (
        competition_data
        .pipe(lambda x: x[x['sampled_price'] == x['sampled_price'].max()])
        .sample(frac=1, random_state=rng.integers(0, 1e8))
        .tail(1)
        .pipe(lambda y: {
            'item': y['item'].values[0],
            'price': int(y['sampled_price'].values[0])}))
def get_inventory_item_price_bounds():
    return (
        query_data(
            """
            with max_item_rowids as (
                select
                    item,
                    max(rowid) as max_rowid
                from price_bound_history
                group by
                    item),
            latest_price_bounds as (
                select
                    a.item,
                    b.low,
                    b.high
                from max_item_rowids a
                left outer join price_bound_history b on
                    a.item = b.item
                    and a.max_rowid = b.rowid),
            inventory_items as (
                select
                    item
                from inventory
                where
                    count > 0)

            select
                item,
                low,
                high
            from latest_price_bounds
            where
                 item in inventory_items
            """))
def update_shelf_history(shelf_id, item, price):
    query_data_without_output(
        query=f"""
            insert into shelf_history values(
                :shelf_id,
                :item,
                :price)
        """,
        params={
            'shelf_id': shelf_id,
            'item': item,
            'price': price})
def add_item_2_shelf_and_set_price(shelf_id, item, price):
    query_data_without_output(
        query="""
            update shelves
            set
                item = :item,
                price = :price
            where
                id = :shelf_id;""",
        params={
            'shelf_id': shelf_id,
            'item': item,
            'price': price})
def move_item_2_shelf_and_set_price(item, shelf_id, price):
    bulk_update_inventory_changes(
        item_changes={item: -1})
    update_inventory()
    update_shelf_history(
        shelf_id=shelf_id,
        item=item,
        price=price)
    add_item_2_shelf_and_set_price(
        shelf_id=shelf_id,
        item=item,
        price=price)
def fill_empty_shelves_w_priced_items(rng):
    for empty_shelf_id in get_empty_shelf_ids():
        competition_data = (
            get_thompson_competition(
                price_bounds=get_inventory_item_price_bounds(),
                rng=rng,
                next_competition_index=get_next_competition_index()))
        add_thompson_competition(
            competition_data=competition_data.to_dict('records'))
        suggested_shelf_changes = (
            get_thompson_sampled_item_and_price(
                competition_data=competition_data,
                rng=rng)
            | {'shelf_id': empty_shelf_id})
        move_item_2_shelf_and_set_price(
            item=suggested_shelf_changes['item'],
            shelf_id=suggested_shelf_changes['shelf_id'],
            price=suggested_shelf_changes['price'])
def get_inventory_item_count():
    return (
        query_data("""
            select
                sum(count) as inventory_item_count
            from inventory
        """)
        ['inventory_item_count']
        .pipe(lambda x: int(x.values[0])))
def get_shelf_item_count():
    return (
        query_data("""
            select
                count(*) as non_null_count
            from shelves
            where
                item is not null
        """)
        ['non_null_count']
        .pipe(lambda x: int(x.values[0])))
def get_occupied_shelf_ids():
    return (
        query_data("""
            select distinct
                id
            from shelves
            where
                item is not null
        """)
        ['id']
        .pipe(lambda x: [int(y) for y in x.values]))
def choose_random_occupied_shelf(rng):
    return (
        pipe(
            get_occupied_shelf_ids(),
            rng.choice,
            int))
def get_random_shelf_reaction(rng):
    return (
        query_data(
            """
            with chosen_shelf_data as (
                select
                    *
                from shelves
                where
                    id = :shelf_id)

            select
                a.id as shelf_id,
                case
                    when a.price <= b.cheap_upper
                    then 'ecstatic'
                    when b.cheap_upper < a.price and a.price <= b.perfect_upper
                    then 'content'
                    when b.perfect_upper < a.price and a.price <= b.expensive_upper
                    then 'sad'
                    else 'angry'
                end as mood
            from chosen_shelf_data a
            left outer join price_reaction_bounds b on
                a.item = b.item
            """,
            {'shelf_id': choose_random_occupied_shelf(rng)})
        .to_dict('records')
        [0])
def record_reaction(shelf_id, mood):
    query_data_without_output(
        """
        with shelf_data as (
            select
                id,
                item,
                price
            from shelves
            where
                id = :shelf_id)

        insert into reactions values(
            :shelf_id,
            (select item from shelf_data where id = :shelf_id),
            (select price from shelf_data where id = :shelf_id),
            :mood)
        """,
        {
            'shelf_id': shelf_id,
            'mood': mood})
def update_price_bound_history():
    query_data_without_output(
        """
        insert into price_bound_history
        with latest_reaction as (
            select
                rowid as reaction_id,
                item,
                price,
                mood
            from reactions
            where
                rowid = (select max(rowid) from reactions)),
        latest_reaction_item_price_bounds as (
            select
                item,
                low,
                high
            from price_bound_history
            where
                item = (select item from latest_reaction)
                and rowid = (
                    select
                        max(rowid)
                    from price_bound_history
                    where
                        item = (select item from latest_reaction))),

        price_bounds_w_reaction as (
            select
                a.reaction_id,
                a.item,
                a.price,
                a.mood,
                b.low,
                b.high
            from latest_reaction a
            left outer join latest_reaction_item_price_bounds b on
                a.item = b.item)

        select
            reaction_id,
            item,
            case
                when mood in ('content', 'ecstatic')
                then price + 1
                when mood = 'sad' and low != high
                then price
                else low
            end as low,
            case
                when mood = 'angry' and low != high
                then price - 1
                else high
            end as high
        from price_bounds_w_reaction""")
def get_shelf_item_and_price(shelf_id):
    return (
        query_data(
            """
            select
                item,
                price
            from shelves
            where
                id = :shelf_id
            """,
            {'shelf_id': shelf_id})
        .to_dict('records')
        [0])
def empty_shelf(shelf_id):
    query_data_without_output(
        query="""
            update shelves
            set
                item = null,
                price = null
            where
                id = :shelf_id;""",
        params={'shelf_id': shelf_id})
    query_data_without_output(
        query=f"""
            insert into shelf_history values(
                :shelf_id,
                null,
                null)
        """,
        params={
            'shelf_id': shelf_id})
def get_price_bound_violating_shelf_ids():
    return (
        query_data(
            """
            with latest_item_rowids as (
                select
                    max(rowid) as max_rowid,
                    item
                from price_bound_history
                group by
                    item),
            latest_price_bounds as (
                select
                    a.item,
                    b.low,
                    b.high
                from latest_item_rowids a
                left outer join price_bound_history b on
                    a.max_rowid = b.rowid
                    and a.item = b.item)
            select
                a.id
            from shelves a
            left outer join latest_price_bounds b on
                a.item = b.item
            where
                a.price < b.low
                or a.price > b.high
            """
        )
        ['id']
        .pipe(lambda x: [int(y) for y in x.values]))
def replace_items_on_shelf_violating_price_bounds():
    for violating_shelf_id in get_price_bound_violating_shelf_ids():
        violating_shelf_item_and_price = get_shelf_item_and_price(violating_shelf_id)
        empty_shelf(shelf_id=violating_shelf_id)
        add_items_2_inventory(
            item_counts={violating_shelf_item_and_price['item']: 1})
        competition_data = (
            get_thompson_competition(
                price_bounds=get_inventory_item_price_bounds(),
                rng=rng,
                next_competition_index=get_next_competition_index()))
        add_thompson_competition(
            competition_data=competition_data.to_dict('records'))
        violating_shelf_replacement_and_price = (
            get_thompson_sampled_item_and_price(
                competition_data=competition_data,
                rng=rng))
        move_item_2_shelf_and_set_price(
            item=violating_shelf_replacement_and_price['item'],
            shelf_id=violating_shelf_id,
            price=violating_shelf_replacement_and_price['price'])

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
    initialize_price_bound_history({
        '275|3000': [
            'gold_runes', 'hardened_steel'],
        '2|275': [
            'broken_sword', 'ancient_pot', 'crystallized_energy',
            'glass_lenses', 'golem_core', 'iron_bar', 'root', 'teeth_stone', 'vine']})
    add_price_reaction_bounds(
        price_reaction_bounds=(
            pd.DataFrame(
                [
                    ['broken_sword', 134, 165, 173],
                    ['crystallized_energy', 89, 110, 115],
                    ['glass_lenses', 89, 110, 115],
                    ['gold_runes', 269, 330, 345],
                    ['golem_core', 89, 110, 115],
                    ['hardened_steel', 269, 330, 345],
                    ['iron_bar', 21, 28, 30],
                    ['root', 3, 6, 8],
                    ['teeth_stone', 3, 6, 8],
                    ['vine', 0, 3, 5]],
                columns=[
                    'item', 'cheap_upper', 'perfect_upper',
                    'expensive_upper'])
            .to_dict('records')))
    rng = np.random.default_rng(71071763)
    fill_empty_shelves_w_priced_items(rng)
    inventory_item_count = get_inventory_item_count()
    shelf_item_count = get_shelf_item_count()
    while inventory_item_count or shelf_item_count:
        shelf_reaction = get_random_shelf_reaction(rng)
        record_reaction(
            shelf_id=shelf_reaction['shelf_id'],
            mood=shelf_reaction['mood'])
        update_price_bound_history()
        if shelf_reaction['mood'] == 'angry':
            add_items_2_inventory(
                item_counts={
                    get_shelf_item_and_price(shelf_id=shelf_reaction['shelf_id'])['item']: 1})
        empty_shelf(shelf_id=shelf_reaction['shelf_id'])
        fill_empty_shelves_w_priced_items(rng)
        replace_items_on_shelf_violating_price_bounds()
        inventory_item_count = get_inventory_item_count()
        shelf_item_count = get_shelf_item_count()
