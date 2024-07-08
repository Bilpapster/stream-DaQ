import pathway as pw


# This is the base code that needs to be written for both streaming and static modes.
# The codebase is the same for both of them, k-architecture.
# Also, pathway employs incremental computation! Note: we also want this for stream-daq
class SchemaT1(pw.Schema):
    name: str
    age: int


class SchemaT2(pw.Schema):
    name: str
    country: str


def pipeline(T1, T2):
    T1bis = T1.select(*pw.this, adult=pw.apply(lambda x: x > 18, pw.this.age))
    T2bis = T2.filter(pw.this.country == "US")
    T3 = T1bis.join(T2bis, pw.left.name == pw.right.name).select(
        pw.left.name, pw.left.adult
    )
    return (T2bis, T3)


# Code for STREAMING scenario
# Note: Streaming mode is infinite, pathway runs forever until the process is killed.
# Node: Streaming mode is the default mode pathway is designed for being run.

T1 = pw.io.csv.read("input_dir_1.csv", schema=SchemaT1, mode="streaming")
T2 = pw.io.csv.read("input_dir_2.csv", schema=SchemaT2, mode="streaming")
T2bis, T3 = pipeline(T1, T2)
pw.io.csv.write(T2bis, "output_directory_for_T2bis.csv")
pw.io.csv.write(T3, "output_directory_for_T3.csv")
pw.run()
