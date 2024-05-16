import pandas as pd
import numpy as np
import sqlite3
from moonlighter_pricing import (
    move_allowed_tables_to_end,
    pipe,
    use_map,
    get_thompson_competition,
    get_thompson_sampled_item_and_price)

def get_test_rng():
    return np.random.default_rng(71071763)
def get_test_thompson_competition():
    return (
        pd.DataFrame([
            {
                'competition_ind': 4,
                'item': 'gold_runes',
                'price_lower_bound': 275,
                'price_upper_bound': 3000,
                'sampled_price': 917},
            {
                'competition_ind': 4,
                'item': 'hardened_steel',
                'price_lower_bound': 275,
                'price_upper_bound': 3000,
                'sampled_price': 495},
            {
                'competition_ind': 4,
                'item': 'broken_sword',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 223},
            {
                'competition_ind': 4,
                'item': 'ancient_pot',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 79},
            {
                'competition_ind': 4,
                'item': 'crystallized_energy',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 192},
            {
                'competition_ind': 4,
                'item': 'glass_lenses',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 109},
            {
                'competition_ind': 4,
                'item': 'golem_core',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 90},
            {
                'competition_ind': 4,
                'item': 'iron_bar',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 161},
            {
                'competition_ind': 4,
                'item': 'root',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 134},
            {
                'competition_ind': 4,
                'item': 'teeth_stone',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 71},
            {
                'competition_ind': 4,
                'item': 'vine',
                'price_lower_bound': 2,
                'price_upper_bound': 275,
                'sampled_price': 247}]))
def test_get_thompson_sampled_item_and_price():
    assert (
        get_thompson_sampled_item_and_price(
            competition_data=get_test_thompson_competition(),
            rng=get_test_rng())
        == {'item': 'gold_runes', 'price': 917})
def test_get_thompson_competition():
    assert (
        get_thompson_competition(
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
            rng=get_test_rng(),
            next_competition_index=4)
        .equals(
            get_test_thompson_competition()))
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
    print(test_get_thompson_sampled_item_and_price())
