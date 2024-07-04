import csv


def main():
    field_names = [
        'timestamp',
        'max',
        'min',
        'mean',
        'count',
        'distinct'
    ]
    with open('data.csv', 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()


if __name__ == '__main__':
    main()
