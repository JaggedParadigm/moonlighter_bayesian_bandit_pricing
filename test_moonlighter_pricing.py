import pandas as pd
import numpy as np
import sqlite3
from moonlighter_pricing import (
    move_allowed_tables_to_end,
    pipe,
    use_map,
    choose_item_and_price
)

def test_choose_item_and_price():
    assert (
        choose_item_and_price(
            price_bounds=(
                pd.DataFrame(
                    [
                        ['gold_runes', 275, 3000],
                        ['hardened_steel', 275, 3000],
                        ['broken_sword', 2, 275],
                        ['ancient_pot', 2, 275],
                        ['crystallized_energy', 2, 275],
                        ['glass_lenses', 2, 275],
                        ['golem_core', 2, 275],
                        ['iron_bar', 2, 275],
                        ['root', 2, 275],
                        ['teeth_stone', 2, 275],
                        ['vine', 2, 275]],
                    columns=['item', 'low', 'high'])),
            rng=np.random.default_rng(71071763))
        == {'item': 'gold_runes', 'price': 917})
def test_move_allowed_tables_to_end():
    assert (
        move_allowed_tables_to_end([
            'a', 'b', 'allowed_c',
            'd', 'allowed_e', 'f',
            'g', 'h'])
        == [
            'a', 'b', 'd', 'f', 'g', 'h',
            'allowed_c', 'allowed_e'])
def test_pipe():
    assert(
        pipe(
            5,
            lambda x: 2 * x,
            lambda x: x + 1)
        == 11)
def test_use_map():
    assert(
        pipe(
            [1, 2, 3],
            use_map(lambda x: x + 5),
            use_map(lambda x: 2 * x))
        == [12, 14, 16])

if __name__ == '__main__':
    test_choose_item_and_price()
