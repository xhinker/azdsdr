# region pandas tool
from IPython.display import display
from IPython.core.display import HTML

class pd_tools:
    def __init__(self) -> None:
        pass

    def display_all(self,df_data,top=-1):
        style = '''
        <style>.dataframe td { 
            text-align: left; 
            max-width: 400px;
        }</style>'''
        if top == -1:
            display(HTML(style + df_data.to_html()))
        elif top>0:
            display(HTML(style + df_data.head(top).to_html()))
        else:
            print('The top parameter should be larger than 0.')

# endregion

# region vis tools
import matplotlib.pyplot as plt
from matplotlib import rcParams
import matplotlib

class vis_tools:
    def __init__(
        self
        ,w = 15
        ,h = 10
        ,font_family = 'sans-serif'
        ,font_name = None
    ) -> None:
        #rcParams['font.sans-serif']     = "Comic Sans MS"
        if font_name:
            rcParams['font.family']         = font_family
            rcParams['font.sans-serif']     = font_name
        self.csfont                     = {'fontname':'Comic Sans MS'}
        self.hfont                      = {'fontname':'Helvetica'}
        self.msfont                     = {'fontname':'Segoe UI'}
        self.msfont_light               = {'fontname':'Segoe UI Light'}
        self.label_text_font            = {'size':'20','weight':'bold'}
        self.small_label_font           = {'size':'18','weight':'normal'}
        self.colors_seq                 = ['#003366','#A8C5E0','green','lime','sienna','violet']
        self.title_size                 = 20 
        self.axis_size                  = 18 
        self.label_size                 = 12 
        self.note_size                  = 18 
        self.dark_blue                  = "#003366"
        self.light_blue                 = "#A8C5E0"
        self.size_w                     = w
        self.size_h                     = h

    def human_format(self,num):
        magnitude = 0 
        while abs(num) >= 1000:
            magnitude += 1 
            num /= 1000.0 
        # add more suffixes if you need them 
        return '%.0f%s' % (num, ['', 'K', 'M', 'G', 'T', 'P'][magnitude])

    def single_bar_chart(
        self
        ,title
        ,x_list
        ,y_list
        ,rotate_x = 30
    ):
        '''
        Show one set of bar chart from x and y input
        '''
        fig,ax      = plt.subplots() 
        fig.set_size_inches(self.size_w, self.size_h)
        fig.autofmt_xdate(rotation=rotate_x)

        x_axis_len  = len(x_list) 
        ax.bar(
            range(x_axis_len)
            ,y_list
            ,color="#003366"
            ,width = 0.2
        ) 
        ax.set_xticks(range(x_axis_len))
        ax.set_xticklabels(x_list)
        ax.xaxis.set_tick_params(labelsize=self.label_size)
        ax.yaxis.set_tick_params(labelsize=self.label_size)

        # set style 
        ax.set_title(
            title
            ,fontsize=self.title_size
            #,**self.msfont
        )
        ax.grid(False) 
        ax.set_facecolor('w') 

        #set label value
        y_max = max(y_list)
        for i,v in enumerate(y_list):
            ax.text(i
                    ,v+y_max/100
                    ,f"{v:,d}"
                    ,color = '#080808'
                    ,fontweight = 'normal'
                    ,ha = 'center'
            )
        
        ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p:self.human_format(x)))
        return ax 
    
    def single_barh_chart(
        self
        ,title
        ,x_list                         # the labels data
        ,y_list                         # the values data
    ): 
        '''
        Show single set of horizontal bar chart
        '''
        fig,ax          = plt.subplots() 
        fig.set_size_inches(self.size_w, self.size_h) 

        x_list_index    = range(len(x_list))
        ax.barh(x_list_index,y_list,color=self.dark_blue) 
        ax.set_yticks(x_list_index)
        ax.set_yticklabels(x_list)
        
        # set style 
        ax.set_title(title,fontsize=self.title_size)
        ax.grid(False) 
        ax.set_facecolor('w') 

        y_max           = max(y_list)
        ax.xaxis.set_tick_params(labelsize=self.label_size)
        ax.yaxis.set_tick_params(labelsize=self.label_size)
            
        for i,v in enumerate(y_list):
            ax.text(v+y_max/20,i-0.2, self.human_format(v))
            ax.set_xticks([])    
        
        ax.spines[['top','right','bottom']].set_visible(False)

        return ax 

    def line1_chart(
        self
        ,title
        ,x_list
        ,y_list
        ,line1_name = ''
    ):
        fig,ax          = plt.subplots() 
        fig.set_size_inches(self.size_w, self.size_h)
        fig.autofmt_xdate(rotation=45)

        ax.set_title(
            title
            ,fontsize=self.title_size
        )
        ax.grid(False) 
        ax.set_facecolor('w') 
        ax.xaxis.set_tick_params(labelsize=self.label_size)
        ax.yaxis.set_tick_params(labelsize=self.label_size)

        x_label_index   = range(len(x_list))
        ax.plot(x_label_index,y_list,color=self.dark_blue)
        plt.xticks(x_label_index) #show all x labels
        ax.set_xticklabels(x_list)
        ax.set_ylim(bottom=0)

        ax.text(max(x_label_index),y_list[-1],line1_name,**self.label_text_font,color=self.dark_blue)
        #ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p:self.human_format(x)))
        return ax

    def line2_chart(
        self
        ,title
        ,x_list
        ,y1_list
        ,y2_list
        ,line1_name = ''
        ,line2_name = ''
    ):
        fig,ax          = plt.subplots() 
        fig.set_size_inches(self.size_w, self.size_h)
        fig.autofmt_xdate(rotation=45)

        x_label_index   = range(len(x_list))

        ax.set_title(title,fontsize=self.title_size)
        ax.grid(False) 
        ax.set_facecolor('w') 
        ax.xaxis.set_tick_params(labelsize=self.label_size)
        ax.yaxis.set_tick_params(labelsize=self.label_size)

        ax.plot(x_label_index,y1_list,color=self.dark_blue)
        ax.plot(x_label_index,y2_list,color=self.light_blue)
        plt.xticks(x_label_index)
        ax.set_xticklabels(x_list)
        ax.set_ylim(bottom=0)

        ax.text(max(x_label_index),y1_list[-1],line1_name,**self.label_text_font,color=self.dark_blue)
        ax.text(max(x_label_index),y2_list[-1],line2_name,**self.label_text_font,color=self.light_blue)
        #ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        ax.get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p:self.human_format(x)))
        return ax

# endregion