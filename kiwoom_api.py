from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import sys
import time
import pandas as pd
import fid
from fid import FID_CODES, get_fid
import gpt
import Visualizer
import crowler
import os
# sys.path.insert(0, os.path.abspath("C:\\Users\\jungh\\PycharmProjects\\Quant_Model"))
# from traing import AI_model

class Kiwoom(QAxWidget):
    def __init__(self):
        super().__init__()
        self._make_kiwoom_instance()
        self._set_signal_slots()
        self._comm_connect()
        self.account_number = self.get_account_number()
        self.tr_event_loop = QEventLoop()
        self.universe_realtime_transaction_info = []
        self.tr_data = None

    def _make_kiwoom_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def _set_signal_slots(self):
        self.OnEventConnect.connect(self._login_slot)
        self.OnReceiveTrData.connect(self.on_receive_tr_data)
        self.OnReceiveMsg.connect(self._on_receive_msg)
        self.OnReceiveChejanData.connect(self._on_receive_chejan)
        self.OnReceiveRealData.connect(self._on_receive_real_data)

    def set_real_reg(self, str_screen_no, str_code_list, str_fid_list, str_opt_type):
        self.dynamicCall("SetRealReg(QString, QString, QString, QString)", str_screen_no, str_code_list, str_fid_list,
                         str_opt_type)
        time.sleep(1)

    def _login_slot(self, err_code):
        if err_code == 0:
            print("Connected!")
        else:
            print("Not Connected...")
        self.login_event_loop.exit()

    def _comm_connect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop = QEventLoop()
        self.login_event_loop.exec_()

    def get_account_number(self):
        account_list = self.dynamicCall("GetLoginInfo(QString)", "ACCLIST")
        account_number = account_list.split(';')[0]
        print(account_number)
        return account_number

    def get_code_list_stock_market(self, market_type):
        code_list = self.dynamicCall("GetCodeListByMarket(QString)", market_type)
        code_list = code_list.split(';')[:-1]
        return code_list

    def get_code_name(self, code):
        name = self.dynamicCall("GetMasterCodeName(QString)", code)
        return name

    def _on_receive_real_data(self, s_code, real_type, real_data):
        if real_type == "장시작시간":
            pass
        elif real_type == "주식체결":
            signed_at = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("체결시간"))
            close = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("현재가"))
            close = abs(int(close))

            high = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("고가"))
            high = abs(int(high))

            open = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("시가"))
            open = abs(int(open))

            low = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("저가"))
            low = abs(int(low))

            top_priority_ask = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매도호가"))
            top_priority_ask = abs(int(top_priority_ask))

            top_priority_bid = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("(최우선)매수호가"))
            top_priority_bid = abs(int(top_priority_bid))

            accum_volume = self.dynamicCall("GetCommRealData(QString, int)", s_code, get_fid("누적거래량"))
            accum_volume = abs(int(accum_volume))

            self.universe_realtime_transaction_info.append(
                [s_code, signed_at, close, high, open, low, top_priority_ask, top_priority_bid, accum_volume])

    # print(s_code, open, high, low, close, top_priority_ask, top_priority_bid, accum_volume)

    def _on_receive_msg(self, screen_no, rqname, trcode, msg):
        print(screen_no, rqname, trcode, msg)

    def _on_receive_chejan(self, gubun, cnt, fid_list):
        print(gubun, cnt, fid_list)

        for fid in fid_list.split(";"):
            code = self.dynamicCall("GetChejanData(int)", "9001")[1:]
            data = self.dynamicCall("GetChejanData(int)", fid).lstrip("+").lstrip("-")
            if data.isdigit():
                data = int(data)
            name = FID_CODES[fid]
            print('{} : {}'.format(name, data))

    def get_price(self, code):
        self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10081", "opt10081", 0, "0020")
        self.tr_event_loop.exec_()
        time.sleep(1)

        total = self.tr_data

        while self.isnext:
            self.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
            self.dynamicCall("SetInputValue(QString, QString)", "수정주가구분", "1")
            self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10081", "opt10081", 2, "0020")
            self.tr_event_loop.exec_()
            total += self.tr_data
            time.sleep(1)

        df = pd.DataFrame(total, columns=['date', 'open', 'high', 'low', 'close', 'volume']).set_index("date")
        df = df.drop_duplicates()
        df = df.sort_index()
        return df

    def get_deposit(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "2")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00001", "opw00001", 0, "0002")
        self.tr_event_loop.exec_()
        return self.tr_data

    def can_trade(self, quantity, price):  # 나중에는 실시간 가격으로 변경가능
        deposit = self.get_deposit()
        if deposit >= quantity * price:
            return True
        else:
            return False

    def get_balance(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "1")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opw00018", "opw00018", 0, "0032")
        self.tr_event_loop.exec_()
        self.balance_data = pd.DataFrame(self.tr_data)
        return self.tr_data

    def can_sell(self, sell_code):
        # Check if sell_code exists in the first column (index 0)
        if sell_code in self.balance_data[0].values:
            # Filter the DataFrame for the row where the first column matches 'sell_code'
            value = self.balance_data.loc[self.balance_data[0] == sell_code, 2].values[0]
            return value
        else:
            print(f"{sell_code} isn't in balance")
            return None

    def get_order(self):
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_number)
        self.dynamicCall("SetInputValue(QString, QString)", "전체종목구분", "0")
        self.dynamicCall("SetInputValue(QString, QString)", "체결구분", "0")
        self.dynamicCall("SetInputValue(QString, QString)", "매매구분", "0")
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "opt10075", "opt10075", 0, "0002")
        self.tr_event_loop.exec_()
        return self.tr_data

    def send_order(self, rqname, screen_no, order_type, code, order_quantity, order_price, order_gubun, order_no=""):
        # SendOrder 메서드를 호출하기 위해 인자를 올바르게 포맷합니다.
        order_result = self.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            [rqname, screen_no, self.account_number, order_type, code, order_quantity, order_price, order_gubun,
             order_no]
        )
        return order_result

    def get_comm_data(self, trcode, record, index, item):
        return self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, record, index, item)

    def on_receive_tr_data(self, screen_no, rqname, trcode, record_name, next, v1, v2, v3, v4):
        print(screen_no, rqname, trcode, record_name, next)
        cnt = self.dynamicCall("GetRepeatCnt(QString,QString)", trcode, rqname)

        if next == "2":
            self.isnext = True  # 조회할 페이지가 남으면 2
        else:
            self.isnext = False

        if rqname == "opt10081":
            total = []
            for i in range(cnt):
                date = self.get_comm_data(trcode, rqname, i, "일자").strip()
                open = int(self.get_comm_data(trcode, rqname, i, "시가").strip())
                high = int(self.get_comm_data(trcode, rqname, i, "고가").strip())
                low = int(self.get_comm_data(trcode, rqname, i, "저가").strip())
                close = int(self.get_comm_data(trcode, rqname, i, "현재가").strip())
                volume = int(self.get_comm_data(trcode, rqname, i, "거래량").strip())
                total.append([date, open, high, low, close, volume])
                print(total)
            self.tr_data = total

        elif rqname == 'opw00001':
            deposit = self.dynamicCall("GetCommData(QString, QString, int,QString)", trcode, rqname, 0, "주문가능금액")
            self.tr_data = int(deposit)

        elif rqname == "opt10075":
            box = []
            for i in range(cnt):
                code = self.get_comm_data(trcode, rqname, i, "종목코드")
                code_name = self.get_comm_data(trcode, rqname, i, "종목명")
                order_number = self.get_comm_data(trcode, rqname, i, "주문번호")
                order_status = self.get_comm_data(trcode, rqname, i, "주문상태")
                order_quantity = self.get_comm_data(trcode, rqname, i, "주문수량")
                order_price = self.get_comm_data(trcode, rqname, i, "주문가격")
                current_price = self.get_comm_data(trcode, rqname, i, "현재가")
                order_type = self.get_comm_data(trcode, rqname, i, "주문구분")
                left_quantity = self.get_comm_data(trcode, rqname, i, "미체결수량")
                executed_quantity = self.get_comm_data(trcode, rqname, i, "체결량")
                ordered_at = self.get_comm_data(trcode, rqname, i, "시간")
                fee = self.get_comm_data(trcode, rqname, i, "당일매매수수료")
                tax = self.get_comm_data(trcode, rqname, i, "당일매매세금")

                code = code.strip()
                code_name = code_name.strip()
                order_number = str(int(order_number.strip()))
                order_status = order_status.strip()
                order_quantity = int(order_quantity.strip())
                order_price = int(order_price.strip())
                current_price = int(current_price.strip().lstrip("+").lstrip("-"))
                order_type = order_type.strip().lstrip("+").lstrip("-")
                left_quantity = int(left_quantity.strip())
                executed_quantity = int(executed_quantity.strip())
                ordered_at = ordered_at.strip()
                fee = int(fee)
                tax = int(tax)

                box.append([code, code_name, order_number, order_status, order_quantity, order_price, current_price,
                            order_type, left_quantity, executed_quantity, ordered_at, fee, tax])

            self.tr_data = box


        elif rqname == "opw00018":

            balance_data = []

            for i in range(cnt):
                code = self.get_comm_data(trcode, rqname, i, "종목번호").strip()

                code = code[1:]  # 종목번호 앞에 있는 'A' 제거

                name = self.get_comm_data(trcode, rqname, i, "종목명").strip()
                quantity = int(self.get_comm_data(trcode, rqname, i, "보유수량").strip())
                purchase_price = int(self.get_comm_data(trcode, rqname, i, "매입가").strip())
                current_price = int(self.get_comm_data(trcode, rqname, i, "현재가").strip())
                eval_profit_loss = int(self.get_comm_data(trcode, rqname, i, "평가손익").strip())
                yield_ratio = float(self.get_comm_data(trcode, rqname, i, "수익률(%)").strip())

                balance_data.append(
                    [code, name, quantity, purchase_price, current_price, eval_profit_loss, yield_ratio])

            self.tr_data = balance_data

        self.tr_event_loop.exit()

        time.sleep(1)


# class Start:
#     def __init__(self):
#         self.app = QApplication(sys.argv)
#         self.kiwoom = Kiwoom()
#         model = AI_model()
#         print(model.should_buy('005930',1))
#
# start = Start()

stock_name = input('종목을 입력해주세요')
Visualizer.Setting(stock_name)
news_dict = crowler.crowl(stock_name)
text = gpt.CompanyAnalyzer(news_dict,stock_name,3)
print(text.analysis_result)

import gpt

text, opinion = gpt.Stock_Gpt()
print(text)
if opinion == '0':
    cj_order = kiwoom.send_order("buy", "0012", 0, "001040", 1, 123000, 0, order_no="")
elif opinion == '1':
    print("Cj 판매")
elif opinion == '2':
    print("cj 관망중")