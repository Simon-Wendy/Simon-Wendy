import datetime
import numpy as np
import pandas as pd
from autoSendEmail import AutSendEmail
import xlsxwriter


class payDailyConsume(object):
    def __init__(self):
        self.path_day = datetime.datetime.now()
        self.now_day = datetime.datetime.strftime(self.path_day - datetime.timedelta(days=1), '%Y%m%d')
        self.yet_day = datetime.datetime.strftime(self.path_day - datetime.timedelta(days=2), '%Y%m%d')
        self.week_day = datetime.datetime.strftime(self.path_day - datetime.timedelta(days=7), '%Y%m%d')
        self.qtime = datetime.datetime.strftime(self.path_day - datetime.timedelta(days=180), '%Y%m%d')

    def handlData(self):
        path = '../data/{}.csv'.format(datetime.datetime.strftime(self.path_day, '%Y%m%d'))
        corr_path = '../data/8月对应关系.xlsx'
        corr_path1 = '../data/对应关系.xlsx'

        df = pd.read_csv(path)
        corr_data = pd.read_excel(corr_path)[['账户名称', '部门', '优化部门']]
        corr1 = pd.read_excel(corr_path1)[['公司名称', '简称', '部门']]

        df["总消"] = df["总消费"] - df["凤巢优惠券消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"] - df['阿拉丁CPT消费']
        df["信息流"] = df["原生阿拉丁消费"] + df["原生CPC消费"] + df["原生CPM消费"] + df["百意Feed消费"] + df[
            "百意无线开屏CPC消费"] + df["原生GD消费"] + df["百意FeedGD消费"] - df["原生CPC优惠券消费"] - df["原生CPM优惠券消费"] + df["手百开屏消费"]
        df["搜索消费"] = df["搜索点击消费"] + df["凤巢阿拉丁消费"] - df["凤巢优惠券消费"]
        df_all = df[["data_flag", "账户名称", "客户名称", "总消", "信息流", "搜索消费"]].copy()
        data = df_all.groupby(by=['data_flag', '客户名称'])[['总消', '信息流', '搜索消费']].sum().reset_index()
        theConsume = data.groupby(by='data_flag').sum().reset_index()
        # 整体消费情况及环比
        totalCon = theConsume.loc[theConsume['data_flag'] == int(self.now_day), '总消'].values[0]
        feed = theConsume.loc[theConsume['data_flag'] == int(self.now_day), '信息流'].values[0]
        sear = theConsume.loc[theConsume['data_flag'] == int(self.now_day), '搜索消费'].values[0]
        totalRato = totalCon - theConsume.loc[theConsume['data_flag'] == int(self.yet_day), '总消'].values[0]
        totalWeek = totalCon - theConsume.loc[theConsume['data_flag'] == int(self.week_day), '总消'].values[0]
        feedRato = feed - theConsume.loc[theConsume['data_flag'] == int(self.yet_day), '信息流'].values[0]
        searRato = sear - theConsume.loc[theConsume['data_flag'] == int(self.yet_day), '搜索消费'].values[0]

        df_cloumns = ['部门', '总消费', '环比', '周同比']
        df_data = []
        sign_data = []
        # 整体消费情况
        all_text_data = '昨日总消费' + str(np.round(totalCon / 10000, 1)) + '万,环比' + self.ratoIncrease(totalRato) + \
                        '信息流消费' + str(np.round(feed / 10000, 1)) + '万,环比' + self.ratoIncrease(feedRato) + \
                        '搜索消费' + str(np.round(sear / 10000, 1)) + '万,环比' + self.ratoIncrease(searRato)
        sign_data.append('KA事业部')
        sign_data.append(int(totalCon))
        sign_data.append(int(totalRato))
        sign_data.append(int(totalWeek))
        df_data.append(sign_data)
        # 优化消费情况
        optim_data = pd.merge(df_all, corr_data, on='账户名称', how='left')
        optim_data = optim_data.groupby(by=['data_flag', '优化部门'])['总消'].sum().reset_index()
        optim_depart = ['KA优化一部', 'KA优化二部']
        opt_text = '昨日'
        for dep in optim_depart:
            op_data = []
            opt_now_data = \
                optim_data.loc[
                    (optim_data['data_flag'] == int(self.now_day)) & (optim_data['优化部门'] == dep), '总消'].values[0]
            opt_yet_data = \
                optim_data.loc[
                    (optim_data['data_flag'] == int(self.yet_day)) & (optim_data['优化部门'] == dep), '总消'].values[0]
            depRato = opt_now_data - opt_yet_data
            depWeek = opt_now_data - optim_data.loc[
                (optim_data['data_flag'] == int(self.week_day)) & (optim_data['优化部门'] == dep), '总消'].values[0]
            opt_text = opt_text + '{}消费{}万, {}'.format(dep, np.round(opt_now_data / 10000, 1),
                                                       self.ratoIncrease(depRato))
            op_data.append(dep)
            op_data.append(int(opt_now_data))
            op_data.append(int(depRato))
            op_data.append(int(depWeek))
            df_data.append(op_data)

        now_data = data[data['data_flag'] == int(self.now_day)].groupby(by='客户名称')['总消'].sum().reset_index()
        yet_data = data[data['data_flag'] == int(self.yet_day)].groupby(by='客户名称')['总消'].sum().reset_index().rename(
            columns={'总消': '昨日消费'})
        week_data = data[data['data_flag'] == int(self.week_day)].groupby(by='客户名称')['总消'].sum().reset_index().rename(
            columns={'总消': '上周同期消费'})

        dataFinally = pd.merge(now_data, yet_data, on='客户名称', how='outer')
        dataFinally = pd.merge(dataFinally, week_data, on='客户名称', how='outer')

        newData = self.newClicentConsume(data)
        newData = pd.merge(dataFinally, newData, left_on='客户名称', right_on='公司名称', how='left')
        newData = newData.groupby(by='简称')[['总消', '昨日消费', '上周同期消费']].sum().reset_index()
        newData['环比'] = newData['总消'] - newData['昨日消费']
        newData['周同比'] = newData['总消'] - newData['上周同期消费']

        dataFinally = dataFinally.fillna(0)
        dataFinally = pd.merge(dataFinally, corr1, left_on='客户名称', right_on='公司名称', how='left')
        newCli = dataFinally[dataFinally['部门'].isnull()].reset_index(drop=True)

        dataFinally = dataFinally.groupby(by='简称')[['总消', '昨日消费', '上周同期消费']].sum().reset_index()
        dataFinally['环比'] = dataFinally['总消'] - dataFinally['昨日消费']
        dataFinally['周同比'] = dataFinally['总消'] - dataFinally['上周同期消费']

        if pd.to_datetime(self.now_day, format='%Y%m%d').weekday() >= 5:
            all_3th = dataFinally[(dataFinally['周同比'] > 2000) | (dataFinally['周同比'] < -2000)].sort_values(by='周同比',
                                                                                                          ascending=False).reset_index(
                drop=True)
            new_3th = newData[abs(newData['周同比']) > 300].sort_values(by='周同比').reset_index(drop=True)
        else:
            all_3th = dataFinally[(dataFinally['环比'] > 2000) | (dataFinally['环比'] < -2000)].sort_values(by='环比',
                                                                                                        ascending=False).reset_index(
                drop=True)
            new_3th = newData[abs(newData['环比']) > 300].sort_values(by='环比', ascending=False).reset_index(drop=True)
        all_text = self.allClientConsume(all_3th)
        all_new_text = self.newClientData(new_3th)

        book = xlsxwriter.Workbook('../data/简报_{}.xlsx'.format(self.now_day))
        style1 = book.add_format({'border': 1,
                                  'align': 'center',
                                  'valign': 'vcenter',
                                  'font': '微软雅黑',
                                  'num_format': '#,##0',
                                  'color': 'red', })
        style2 = book.add_format({'border': 1,
                                  'align': 'center',
                                  'valign': 'vcenter',
                                  'font': '微软雅黑',
                                  'color': 'green',
                                  'num_format': '#,##0', })
        style3 = book.add_format({'border': 1,
                                  'align': 'center',
                                  'valign': 'vcenter',
                                  'font': '微软雅黑',
                                  'num_format': '#,##0'})
        style4 = book.add_format({'align': 'left',
                                  'bold': True,
                                  'valign': 'vcenter',
                                  'font': '微软雅黑', })
        booksheet = book.add_worksheet('Total')
        n_row = 0
        booksheet.write(n_row, 0, "日期: {}".format(self.now_day), style4)
        n_row = n_row + 1
        for d in df_cloumns:
            booksheet.write(n_row, df_cloumns.index(d), d, style3)
        n_row = n_row + 1
        for data_col in df_data:
            col = 0
            booksheet.write(n_row, col, data_col[0], style3)
            booksheet.write(n_row, col + 1, data_col[1], style3)
            booksheet.write(n_row, col + 2, data_col[2], style1 if int(data_col[2]) > 0 else style2)
            booksheet.write(n_row, col + 3, data_col[3], style1 if int(data_col[3]) > 0 else style2)

            n_row = n_row + 1
        booksheet.write(n_row, 0, '主要客户消费情况: ', style4)
        n_row = n_row + 1
        clicent_columns = ['公司名称', '昨日总消', '环比', '周同比']
        df2 = all_3th[['简称', '总消', '环比', '周同比']]
        df3 = new_3th[['简称', '总消', '环比', '周同比']]
        for d in clicent_columns:
            booksheet.write(n_row, clicent_columns.index(d), d, style3)
        n_row = n_row + 1
        for c in range(df2.shape[0]):
            booksheet.write(n_row, 0, df2.loc[c, '简称'], style3)
            booksheet.write(n_row, 1, df2.loc[c, '总消'], style3)
            booksheet.write(n_row, 2, df2.loc[c, '环比'], style1 if int(df2.loc[c, '环比']) > 0 else style2)
            booksheet.write(n_row, 3, df2.loc[c, '周同比'], style1 if int(df2.loc[c, '周同比']) > 0 else style2)
            n_row = n_row + 1
        booksheet.write(n_row, 0, '新户消费情况: ', style4)
        n_row = n_row + 1
        for d in clicent_columns:
            booksheet.write(n_row, clicent_columns.index(d), d, style3)
        n_row = n_row + 1
        for c in range(df3.shape[0]):
            booksheet.write(n_row, 0, df3.loc[c, '简称'], style3)
            booksheet.write(n_row, 1, df3.loc[c, '总消'], style3)
            booksheet.write(n_row, 2, df3.loc[c, '环比'], style1 if int(df3.loc[c, '环比']) > 0 else style2)
            booksheet.write(n_row, 3, df3.loc[c, '周同比'], style1 if int(df3.loc[c, '周同比']) > 0 else style2)
            n_row = n_row + 1
        if not newCli.empty:
            for nc in range(newCli.shape[0]):
                writedata = '新增 {} 消费{}; '.format(newCli.loc[nc, '客户名称'], int(newCli.loc[nc, '总消']))
                booksheet.write(n_row, 0, writedata, style4)
                n_row = n_row + 1

        book.close()

        all_text_data = all_text_data + '\n' + opt_text + '\n' + '昨日主要客户消费情况: ' + '\n' + all_text + '主要新户消费情况: ' + '\n' + all_new_text

        return all_text_data + '\n' + newCli.to_string()

    def newClientData(self, data):
        text = ''
        for i in range(data.shape[0]):
            text = text + data.loc[i, '简称'] + '昨日消费' + str(int(data.loc[i, '总消'])) + ', 环比' + self.newRato(
                data.loc[i, '环比']) + '周同比' + self.newRato(data.loc[i, '周同比']) + '\n'
        return text

    def newRato(self, data):
        r = ''
        if data > 0:
            r = '增长'
        else:
            r = '下降'
        return r + str(abs(int(data))) + '; '

    def newClicentConsume(self, data):
        new_path = '../data/季度新户.xlsx'
        new_data = pd.read_excel(new_path)
        new_data['季度'] = pd.PeriodIndex(new_data['季度'], freq='q')
        qdata = pd.DataFrame([self.qtime], columns=['dataflag'])
        qdata['dataflag'] = pd.PeriodIndex(pd.to_datetime(qdata['dataflag'], format='%Y%M%d'), freq='q')
        Q = qdata['dataflag'].values[0]
        newQ = new_data[new_data['季度'] >= Q]
        return newQ

    def allClientConsume(self, data):
        text = ''
        for i in range(data.shape[0]):
            text = text + data.loc[i, '简称'] + '昨日消费' + str(
                np.round(data.loc[i, '总消'] / 10000, 1)) + '万，环比' + self.ratoIncrease(
                data.loc[i, '环比']) + '周同比' + self.ratoIncrease(data.loc[i, '周同比']) + '\n'
        return text

    def ratoIncrease(self, data):
        if data > 0:
            textStr = "增长{}万; ".format(np.round(data / 10000, 1))
        else:
            textStr = "下降{}万; ".format(np.round(abs(data / 10000), 1))
        return textStr

    def go(self):
        data = self.handlData()
        print(data)
        auto = AutSendEmail(data, ['zhangyanqing@ruizhiqi.com', ])
        auto.sendMessage()


if __name__ == '__main__':
    payCon = payDailyConsume()
    payCon.go()
