# region pandas tool
from IPython.display import display
from IPython.core.display import HTML

class pd_tools:
    def __init__(self) -> None:
        pass

    def display_all(self,df_data):
        style = '''
        <style>.dataframe td { 
            text-align: left; 
            max-width: 400px;
        }</style>'''
        display(HTML(style + df_data.to_html()))

# endregion

# region vis tools
import matplotlib.pyplot as plt
from matplotlib import rcParams

class vis_tools:
    def __init__(self) -> None:
        rcParams['font.sans-serif']     = "Segoe UI"
        rcParams['font.family']         = 'sans-serif'
        self.csfont                     = {'fontname':'Comic Sans MS'}
        self.hfont                      = {'fontname':'Helvetica'}
        self.msfont                     = {'fontname':'Segoe UI'}
        self.msfont_light               = {'fontname':'Segoe UI Light'}
        self.label_text_font            = {'size':'20','weight':'bold','fontname':'Segoe UI'}
        self.small_label_font           = {'size':'18','weight':'normal','fontname':'Segoe UI'}
        self.colors_seq                 = ['#003366','#A8C5E0','green','lime','sienna','violet']
        self.title_size                 = 20 
        self.axis_size                  = 18 
        self.label_size                 = 18 
        self.note_size                  = 18 
        self.dark_blue                  = "#003366"
        self.light_blue                 = "#A8C5E0"

    def human_format(num):
        magnitude = 0 
        while abs(num) >= 1000:
            magnitude += 1 
            num /= 1000.0 
        # add more suffixes if you need them 
        return '%.0f%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])

    def single_bar_chart(
        title,x_axis_array,y_axis_array
        ,size_w = 15
        ,size_h = 10
        ,rotate_x = 30
    ):
        fig,ax      = plt.subplots() 
        fig.set_size_inches(size_w, size_h)
        x_axis_len  = len(x_axis_array) 
        ax.bar(
            range(x_axis_len)
            ,y_axis_array
            ,color="#003366"
            ,width = 0.2
        ) 
        ax.set_xticks(range(x_axis_len))
        x_label_rotate = 0
        #rotate x labels 
        if x_axis_len>5:
            x_label_rotate = rotate_x

        ax.set_xticklabels(x_axis_array,rotation=x_label_rotate)

        # set style 
        ax.set_title(title)
        ax.grid(False) 
        ax.set_facecolor('w') 

        #set label value
        y_max = max(y_axis_array)
        for i,v in enumerate(y_axis_array):
            ax.text(i,v+y_max/100, 
                    f"{v:,d}",
                    color = '#080808',
                    fontweight = 'normal',
                    ha = 'center')
        return ax 

# endregion