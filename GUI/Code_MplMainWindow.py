from PyQt5 import QtWidgets, QtCore
from GUI.Ui_MplMainWindow import Ui_MainWindow

class Code_MainWindow(Ui_MainWindow):
    def __init__(self, parent = None):
        super(Code_MainWindow, self).__init__(parent)
        self.setupUi(self)
        self.begin_Button.clicked.connect(lambda: self.on_button(1))
        self.stop_Button.clicked.connect(lambda: self.on_button(2))

    def on_button(self, n):
        print('button {0} clicked'.format(n))



if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui  = Code_MainWindow()
    ui.show()
    sys.exit(app.exec_())