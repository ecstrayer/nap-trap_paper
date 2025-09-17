
import matplotlib.font_manager
import matplotlib.pyplot as plt
import matplotlib.textpath
import matplotlib.ticker
import matplotlib.patches
import matplotlib.transforms
import matplotlib_venn

import pandas as pd 
import numpy as np
import scipy.stats
import math

matplotlib.rcParams['text.usetex'] = False


#globals

g_fig_height = 4.35
g_fig_width = 4.35
g_margin_left = 1
g_margin_bottom = 1
g_axis_width = 3
g_axis_height = 3

from pathlib import Path
package_root = Path(__file__).parent.parent.parent.parent
style_path = package_root / 'doc' / 'figures.mpl_style'
plt.style.use(str(style_path))
#plt.style.use('doc/figures.mpl_style')



def make_figure(fig_height = g_fig_height, fig_width = g_fig_width, margin_left = g_margin_left, margin_bottom = g_margin_bottom, axis_width = g_axis_width, axis_height = g_axis_height):

    fig = plt.figure(figsize=(fig_width / 2.54, fig_height / 2.54), dpi = 600)
    left = margin_left / fig_width
    bottom = margin_bottom / fig_height
    width = axis_width / fig_width
    height = axis_height / fig_height
    ax = fig.add_axes([left, bottom, width, height])
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    return fig, ax



def pvalue_formater(ax, corr, p, n, rsquared = False):

    if p > 0.001:
        p = math.ceil(p * 10000) / 10000
        p =  f'{p:0.2}'
    else:
        p = 10**-300 if p == 0 else p 
        e = int(np.log10(p)) + 1
        p = f'10^{e}'

    pstr = f'p < {p}'

    if corr is None:
        a = f'{pstr} N = {n}'
    else:
        if rsquared:
            corr = corr ** 2
            a = f'R{2} = {corr:.2f} {pstr} N = {n}'
        else:
            a = f'R = {corr:.2f} {pstr} N = {n}'
    ax.text(x = 0.1, y = 1.01, s = a, fontsize = 6, transform = ax.transAxes)
            
    return ax



def make_frame_plot(reporter_name, orf, main_orf_start, label = None):

    y_pos = [3,2,1]
    orf_color = ['#7f3f98', '#00aeef', '#f7941d']

    fig,ax = make_figure(g_fig_height / 3, g_fig_width, 0, 0, g_axis_width + 1, axis_height= g_axis_height / 3)
    ax.add_patch(matplotlib.patches.Rectangle(xy = (main_orf_start ,y_pos[0] - 0.4), height= 0.8, width = 150-main_orf_start, ec = None ,facecolor = orf_color[0], alpha = 0.75))
    ax.vlines(x = main_orf_start, ymin = 1 - 0.75, ymax = 3 + 0.7, linestyles= 'dashed', colors = 'gray' , linewidth = 1)

    plot_end = main_orf_start +  20

    for start,stop, orf_type, frame in orf:
        start = start - 33
        stop = stop - 33
        stop = stop if stop <  plot_end else plot_end
        orf_len = stop - start
        ax.add_patch(matplotlib.patches.Rectangle(xy = (start,y_pos[frame] - 0.4), height= 0.8, width = orf_len, ec = None, facecolor = orf_color[frame], alpha = 0.5))

    ax.set_xlim(0,plot_end)
    #ax.set_xticks(range(0,160,15))
    ax.set_ylim(0,4)
    ax.set_yticks([1,2,3])
    ax.set_yticklabels(['Frame 3', 'Frame 2', 'Frame 1'], fontsize = 8)
    ax.axis('off')


    ax.annotate('Frame 1', xy = (plot_end + 10, y_pos[0]), rotation = 0, annotation_clip = False, verticalalignment = 'center', c = orf_color[0], fontsize = 8)
    ax.annotate('Frame 2', xy = (plot_end + 10, y_pos[1]), rotation = 0, annotation_clip = False, verticalalignment = 'center', c = orf_color[1], fontsize = 8)    
    ax.annotate('Frame 3', xy = (plot_end + 10, y_pos[2]), rotation = 0, annotation_clip = False, verticalalignment = 'center', c = orf_color[2], fontsize = 8)

    if label is not None:
        reporter_name = label

    ax.text(0.5,1.1, reporter_name, ha = 'center', fontsize = 8, transform = ax.transAxes)

    return fig, ax




def plot_plot(x, y, xlabel, ylabel,figargs = None, **kwargs):

    if figargs is None:
        figargs = {}

    fig, ax = make_figure(**figargs)
    ax.set(**kwargs)
    ax.plot(x, y)
    ax.set_xlabel(xlabel, fontsize = 8, labelpad = 1)
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
 

    return fig,ax






def plot_scatter(x, y, xlabel, ylabel, s = 2, alpha = 1, c = None, cmap = 'viridis', same_axis = True, plot_density = True, edgecolor = "none", show_pearsonr = True, set_maxv = True,  maxv = None, fig_ax = None, rasterized = True,rsquared =False, lin_regress = False, **kwargs):

    if plot_density == True:
        xy = np.vstack([x,y])
        z = scipy.stats.gaussian_kde(xy)(xy)
        c = z


    if fig_ax is None:
        fig, ax = make_figure()
        old_ticks = None
    else:
        fig, ax = fig_ax
        old_ticks = ax.get_yticks()
        ax.yaxis.set_major_locator(matplotlib.ticker.AutoLocator())
        ax.xaxis.set_major_locator(matplotlib.ticker.AutoLocator())
        

    m = [np.max(x), np.max(y)] if fig_ax is None else [np.max(x), np.max(y), ax.get_ylim()[-1]]
    max_ax = np.argmax(m)
    maxv = m[-1] if max_ax == 2 else maxv

    ax.set(**kwargs)
    ax.scatter(x, y, s = s, alpha = alpha, c = c, edgecolor = edgecolor, cmap = cmap, rasterized= rasterized)
    ax.autoscale()
    ax.set_xlabel(xlabel, fontsize = 8, labelpad = 1)
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)


    if set_maxv == True:
        if same_axis:
            maxv = np.round(m[max_ax] * 1.2,2) if maxv is None else maxv
            ax.set_xlim([0, maxv])
            ax.set_ylim([0, maxv])
            yticks = ax.get_yticks()
            xticks = ax.get_xticks()
            t = xticks,yticks,old_ticks
            ax.set_xticks(t[max_ax])
            ax.set_yticks(t[max_ax])
        else:
            max_x, max_y = np.round(m[0], 2) * 1.2, np.round(m[1], 2) * 1.2
            ax.set_xlim([0,max_x])
            ax.set_ylim([0,max_y])
            ax.set_xticks(ax.get_xticks())
            ax.set_yticks(ax.get_yticks())


    if show_pearsonr == True:    
        corr,pval = scipy.stats.pearsonr(x, y)
        ax = pvalue_formater(ax = ax, corr = corr, p = pval, n = x.shape[0], rsquared = rsquared)
    
    if lin_regress:
        l = scipy.stats.linregress(x,y)
        f = lambda a: l.slope * a + l.intercept
        xmin, xmax = np.min(x), np.max(x)
        ax.plot([xmin, xmax], [f(xmin), f(xmax)], linewidth = 0.24, c = 'gray')
        ax.text(x = 1, y = 1, s = f'y = {l.slope:2f} + {l.intercept:2f}', transform = ax.transAxes, fontsize = 4)

    return fig, ax




def plot_hist(x, xlabel, ylabel, color = None, vlines = None, bins = 100, alpha = 0.8, cumulative =False, density = False, histtype = 'bar', fig_ax = None, **kwargs):
    if fig_ax is None:
        fig, ax = make_figure()
    else:
        fig, ax = fig_ax
    
    ax.hist(x, bins = bins, alpha = alpha, color = color, cumulative = cumulative, density = density, histtype= histtype, **kwargs)
    ax.set_xlabel(xlabel, fontsize = 8, labelpad = 1)
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)

    if vlines is not None:
        for v in vlines:
            p = np.percentile(x, v)
            ax.axvline(p, color = 'black', linestyle = '--', linewidth = 0.5)

    return fig, ax



def plot_bar(height, yerr, xticks, ylabel, color = None, fig_ax = None, **kwargs):

    feature_dic = {
                    'ATG_oorf_out_of_frame_mean_kozak_score_Hs' : 'ooORF Kozak',
                    'ATG_oorf_out_of_frame_mean_kozak_score_Dr' : 'ooORF Kozak',
                    'ATG_uorf_all_mean_kozak_score_Hs' : 'uORF Kozak',
                    'ATG_oorf_in_frame_mean_orf_length' : 'ioORF length',
                    'ATG_uorf_all_mean_kozak_score_Dr' : 'uORF Kozak',
                    'ATG_oorf_out_of_frame_mean_orf_length' : 'ooORF length',
                    'ATG_oorf_in_frame_mean_kozak_score_Dr' : 'ioORF Kozak',
                    'ATG_uorf_all_mean_orf_length' : 'uORF length',
                    'ATG_oorf_out_of_frame_orf_number' : 'ooORF number',
                    'ATG_oorf_in_frame_orf_number' : 'ioORF number',
                    'ATG_uorf_all_orf_number' : 'uORF number',
                    'ATG_oorf_in_frame_mean_kozak_score_Hs' : 'ioORF Kozak',
                }

    tick_num = len(xticks)
    xticks = [feature_dic[xticks[n]] if xticks[n] in feature_dic else xticks[n].replace('T','U') for n in range(tick_num)]

    if fig_ax is None:
        fig, ax = make_figure()
    else:
        fig,ax = fig_ax
        
    ax.set(**kwargs)
    ax.bar(xticks, height, yerr = yerr, color = color)
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.set_xticklabels(xticks, rotation = 90, fontsize = 6)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)

    return fig, ax





def plot_box(x, xlabels, xticks, ylabel, box_args = {}, **kwargs):

    fig, ax = make_figure()
    ax.set(**kwargs)
    ax.boxplot(x, labels = xlabels, **box_args)
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)
    ax.set_xticklabels(xticks, rotation = 90, fontsize = 6)
    return fig, ax



def plot_venn(set_list, set_labels, **kwargs):
    fig, ax = make_figure()
    ax.set(**kwargs)
    if len(set_labels)  == 3:
        matplotlib_venn.venn3(set_list, set_labels, ax = ax)
    elif len(set_labels)  == 2:
        matplotlib_venn.venn2(set_list, set_labels, ax = ax)

    return fig, ax



def plot_pie(x, labels, title,  **kwargs):
    fig, ax = make_figure()
    ax.set(**kwargs)
    ax.pie(x, labels = labels, title = title)
    return fig, ax


def plot_stacked_bar(data, group_labels, class_labels, ylabel, **kwargs):
    fig, ax = make_figure()
    ax.set(**kwargs)
    ax.tick_params(axis = 'both', which = 'major', labelsize = 6)

    data = data / data.sum(axis = 0)
    data = data.cumsum(axis = 0)
    print(data)

    for i,c in reversed(list(enumerate(class_labels))):
        print(i,c)
        ax.bar(group_labels, data[i,:].reshape(-1), label = c)
    
    ax.set_ylabel(ylabel, fontsize = 8,labelpad = 1)
    ax.legend(fontsize = 6)
    

    return fig, ax