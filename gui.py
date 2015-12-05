import wx

class ComboBoxFrame(wx.Frame):
    def __init__(self):
        wx.Frame.__init__(self, None, -1, 'Select Your Data:', size=(375, 300))
        panel = wx.Panel(self, -1)
        
        diff = 30
        textX = 20
        textY = 15

        fieldX = 170
        fieldY = 10

        countryList = ['India', 'Pakistan', 'USA']
        wx.StaticText(panel, -1, "Country:", (textX, textY))
        self.countryCombo = wx.ComboBox(panel, -1, countryList[0], (fieldX, fieldY), wx.DefaultSize, countryList, wx.CB_DROPDOWN)
        
        textY += 30
        fieldY += 30
        indicatorList = ['indicator1', 'indicator2', 'indicator3']
        wx.StaticText(panel, -1, "Indicator:", (textX, textY))
        self.indicatorCombo = wx.ComboBox(panel, -1, indicatorList[0], (fieldX, fieldY), wx.DefaultSize, indicatorList, wx.CB_DROPDOWN)

        textY += 30
        fieldY += 30
        yearList = range(1970, 2016)  # 0 to 2015
        yearList = map(str, yearList)
        wx.StaticText(panel, -1, "Start year:", (textX, textY))
        self.startYearCombo = wx.ComboBox(panel, -1, yearList[0], (fieldX, fieldY), wx.DefaultSize, yearList, wx.CB_DROPDOWN)

        textY += 30
        fieldY += 30
        wx.StaticText(panel, -1, "End year:", (textX, textY))
        self.endYearCombo = wx.ComboBox(panel, -1, yearList[-1], (fieldX, fieldY), wx.DefaultSize, yearList, wx.CB_DROPDOWN)

        textY += 30
        fieldY += 30
        wx.StaticText(panel, -1, "Prediction end year:", pos=wx.Point(textX, textY))
        self.predEndYearText = wx.TextCtrl(panel, -1, value="2015", pos=wx.Point(fieldX, fieldY), size=wx.Size(175, 25))
                        
        self.button1 = wx.Button(panel, -1, label="Calculate", pos=wx.Point(fieldX, 160), size=wx.Size(175, 28))
        self.button1.Bind(wx.EVT_BUTTON, self.button1Click, self.button1)

    def button1Click(self, event):
        """Conversion button has been clicked"""
        country = self.countryCombo.GetValue()
        indicator = self.indicatorCombo.GetValue()
      	startYear = self.startYearCombo.GetValue()
        endYear = self.endYearCombo.GetValue()
        predYear = self.predEndYearText.GetValue()

        str = "country: %s, indicator: %s, startYear: %s, endYear: %s, predYear: %s" % (country, indicator, startYear, endYear, predYear)
        print str

def getValues():
    app = wx.PySimpleApp()
    ComboBoxFrame().Show()
    app.MainLoop()

if __name__ == "__main__":    
    getValues()

# https://www.daniweb.com/programming/software-development/code/216651/wxpython-combobox-demo
