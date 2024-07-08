import pandas as pd
import matplotlib.pyplot as plt


def main():
    faust = pd.read_csv('data_faust_1.csv')
    bytewax = pd.read_csv('data_bytewax_1.csv', names=[
        'timestamp',
        'window_id',
        'max',
        'min',
        'mean',
        'count',
        'distinct',
        'processing_time'
    ], parse_dates=['timestamp'])
    plt.plot(faust['count'], faust['processing_time'], color='green')
    plt.plot(bytewax['count'], bytewax['processing_time'], color='orange')
    plt.xlabel('Πλήθος στοιχείων ανά παράθυρο ')
    plt.ylabel('Χρόνος επεξεργασίας (sec)')
    plt.legend(['Faust', 'Bytewax'])
    plt.legend(['Faust'])
    plt.show()


if __name__ == '__main__':
    main()
