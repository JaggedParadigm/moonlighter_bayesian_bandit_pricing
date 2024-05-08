import pandas
import sqlite3
from moonlighter_pricing import (
    move_allowed_tables_to_end,
    pipe
)

def test_pipe():
    assert(
        pipe(
            5,
            lambda x: 2 * x,
            lambda x: x + 1)
        == 11)
def test_move_allowed_tables_to_end():
    assert (
        move_allowed_tables_to_end([
            'a', 'b', 'allowed_c',
            'd', 'allowed_e', 'f',
            'g', 'h'])
        == [
            'a', 'b', 'd', 'f', 'g', 'h',
            'allowed_c', 'allowed_e'])

if __name__ == '__main__':
    test_move_allowed_tables_to_end()
