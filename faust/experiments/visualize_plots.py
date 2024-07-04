import pandas as pd
import matplotlib.pyplot as plt


def main():
    faust = pd.read_csv('data_faust_1.csv')
    # plt.plot(range(len(faust['count'])), faust['count'])
    plt.plot(faust['count'], faust['processing_time'])
    plt.xlabel('Πλήθος στοιχείων ανά παράθυρο ')
    plt.ylabel('Χρόνος επεξεργασίας (sec)')
    plt.legend(['Faust'])
    # plt.title(' Χρόνος επεξεργασίας ως προς το πλήθος των στοιχείων εντός παραθύρου')
    plt.show()


if __name__ == '__main__':
    main()
