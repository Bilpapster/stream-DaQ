import pathway as pw

# taken from online documentation https://pathway.com/glossary/tumbling-window
# NOTE: there are some new names employed that are not updated in the documentation
# pw.temporal.tumbling in line 25, instead of pw.window.tumbling
# pw.this._pw_window in line 27, instead of pw.this.window

t = pw.debug.table_from_markdown(
    '''
            | shard | t   | amount | 
        1   | 0     |  12 |   14   |
        2   | 0     |  13 |   11   |
        3   | 0     |  14 |   6    |
        4   | 0     |  15 |   7    |
        5   | 0     |  16 |   90   |
        6   | 0     |  17 |   12   |
        7   | 1     |  12 |   5    |
        8   | 1     |  13 |   2    |
        9   | 0     |  13 |   -2   |
     '''
)
# NOTE that event no9 is out of order for shard 1, yet pathway yields correct results

result = t.windowby(
    t.t, window=pw.temporal.tumbling(duration=5), shard=t.shard
).reduce(
    pw.this._pw_window,
    min_t=pw.reducers.min(pw.this.t),
    max_t=pw.reducers.max(pw.this.t),
    count=pw.reducers.count(pw.this.t),
    max_amount=pw.reducers.max(pw.this.amount),
    min_amount=pw.reducers.min(pw.this.amount)
)

pw.debug.compute_and_print(result, include_id=False)
