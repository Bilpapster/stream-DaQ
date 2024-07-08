def get_chocolate_consumption_data():
    import pathway as pw

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    table = pw.debug.table_from_markdown(
        """
            | time                  | name            | chocolate_bars
         0  | 2023-06-22T09:12:34   | Fudge_McChoc    | 2
         1  | 2023-06-22T09:23:56   | Ganache_Gobbler | 2
         2  | 2023-06-22T09:45:20   | Truffle_Muncher | 1
         3  | 2023-06-22T09:06:30   | Fudge_McChoc    | 1
         4  | 2023-06-22T10:11:42   | Ganache_Gobbler | 2
         5  | 2023-06-22T10:32:55   | Truffle_Muncher | 2
         6  | 2023-06-22T11:07:18   | Fudge_McChoc    | 3
         7  | 2023-06-22T11:23:12   | Ganache_Gobbler | 1
         8  | 2023-06-22T11:49:29   | Truffle_Muncher | 2
         9  | 2023-06-22T12:03:37   | Fudge_McChoc    | 4
         10 | 2023-06-22T12:21:05   | Ganache_Gobbler | 3
         11 | 2023-06-22T13:38:44   | Truffle_Muncher | 3
         12 | 2023-06-22T14:04:12   | Fudge_McChoc    | 1
         13 | 2023-06-22T15:26:39   | Ganache_Gobbler | 4
         14 | 2023-06-22T15:55:00   | Truffle_Muncher | 1
         15 | 2023-06-22T16:18:24   | Fudge_McChoc    | 2
         16 | 2023-06-22T16:32:50   | Ganache_Gobbler | 1
         17 | 2023-06-22T17:58:06   | Truffle_Muncher | 2
        """
    ).with_columns(time=pw.this.time.dt.strptime(TIME_FORMAT))
    return table
