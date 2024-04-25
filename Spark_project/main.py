import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pyspark.sql import SparkSession
import time




class ToolTip(object):
    def __init__(self, widget):
        self.widget = widget
        self.tip_window = None

    def show_tip(self, tip_text):

        if self.tip_window or not tip_text:
            return
        x, y, _cx, cy = self.widget.bbox("insert")
        x = x + self.widget.winfo_rootx() + 25
        y = y + cy + self.widget.winfo_rooty() + 25
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry("+%d+%d" % (x, y))

        label = tk.Label(tw, text=tip_text, justify=tk.LEFT,
                         background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                         font=("tahoma", "10", "normal"))
        label.pack(ipadx=1)

    def hide_tip(self):
        tw = self.tip_window
        self.tip_window = None
        if tw:
            tw.destroy()


# ===================================================================
def create_ToolTip(widget, text):
    toolTip = ToolTip(widget)

    def enter(event):
        toolTip.show_tip(text)

    def leave(event):
        toolTip.hide_tip()

    widget.bind('<Enter>', enter)
    widget.bind('<Leave>', leave)



def app():
    root = tk.Tk()
    root.title("Final_Semester")
    root.configure(bg="#00CCFF")
    root.geometry("1000x1000")
    root.pack_propagate(False)
    root.resizable(True,True)


    frame1 = tk.LabelFrame(root, text="Excel Data")
    frame1.place(height=570, width=3000)


    file_frame = tk.LabelFrame(root, text="Open File",bg="#6699FF",font=("BOLD", 18))
    file_frame.place(height=180, width=1000, rely=0.7, relx=0)


    button1 = tk.Button(file_frame, text="Browse A File", command=lambda: File_dialog(),width=20,height=3,background='#DDDDDD',font=("BOLD",12))
    button1.place(rely=0.45, relx=0.10)

    button2 = tk.Button(file_frame, text="Load File", command=lambda: Load_excel_data(),width=20,height=3, bg= "#DDDDDD",font=("BOLD",12))
    button2.place(rely=0.45, relx=0.64)



    label_file = ttk.Label(file_frame, text="No File Selected",font=("BOLD",12))
    label_file.place(rely=0, relx=0)



    tv1 = ttk.Treeview(frame1)
    tv1.place(relheight=1, relwidth=1)

    treescrolly = tk.Scrollbar(frame1, orient="vertical", command=tv1.yview)
    treescrollx = tk.Scrollbar(frame1, orient="horizontal", command=tv1.xview)
    tv1.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
    treescrollx.pack(side="bottom", fill="x")
    treescrolly.pack(side="right", fill="y")


    def File_dialog():
        filename = filedialog.askopenfilename(initialdir="/",
                                              title="Select A File",
                                              filetype=(("All Files", "*.*"),("xlsx files", "*.xlsx")))
        label_file["text"] = filename
        return None


    def Load_excel_data():
        start = time.time()
        file_path = label_file["text"]
        spark = SparkSession.builder.appName("main").getOrCreate()
        try:
            excel_filename = r"{}".format(file_path)
            df = spark.read.csv(excel_filename, header = True)

        except ValueError:
            tk.messagebox.showerror("Information", "The file you have chosen is invalid")
            return None
        except FileNotFoundError:
            tk.messagebox.showerror("Information", f"No such file as {file_path}")
            return None

        clear_data()
        tv1["column"] = list(df.columns)
        tv1["show"] = "headings"
        for column in tv1["columns"]:
            tv1.heading(column, text=column)

        df_rows = df.collect()
        for row in range(50):
            tv1.insert("", "end", values=df_rows[row])
        end = time.time()
        label_rows = ttk.Label(file_frame, text=f"Rows has found: {df.count()}", font=("BOLD", 12), background="#33FFFF")
        label_rows.place(relx=0.4,rely=0.3)
        label_cols = ttk.Label(file_frame, text=f"Columns has found: {len(df.columns)}", font=("BOLD",12), background="#33FFFF")
        label_cols.place(relx=0.4,rely=0.5)
        label_run_times = ttk.Label(file_frame, text = f"Run time: {int(end - start)}s", font = ("BOLD", 12), background = "#33FFFF")
        label_run_times.place(relx = 0.4, rely = 0.8)
        return None



    def clear_data():
        tv1.delete(*tv1.get_children())
        return None
    root.mainloop()


if __name__ == "__main__":
    app()