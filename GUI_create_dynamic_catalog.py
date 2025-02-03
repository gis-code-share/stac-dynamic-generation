import tkinter as tk
import os, json
from tkinter import filedialog, BooleanVar, messagebox
import subprocess
from customtkinter import CTkButton, CTkCheckBox, CTk, CTkFrame, CTkScrollableFrame
all_files = []

directory = os.path.dirname(os.path.realpath(__file__))
config_directory = "\\config_local"

with open(directory + config_directory + "\\misc_config.json") as f:
    misc_config = json.load(f)

def main():
    global all_files
    root = CTk()  
    root.title("Config File Selector")
    root.geometry("600x550") 

    read_parent_catalog_var = BooleanVar(value=True)
    generate_test_node_var = BooleanVar(value=True)

    def select_files():
        global all_files
        file_paths = filedialog.askopenfilenames(title="Select Config Files",
                                                 filetypes=[("JSON Files", "*.json")])
        print(file_paths)
        all_files.extend(file_paths)
        update_text_area()

    def clear_files():
        global all_files
        all_files = []
        update_text_area(files = False)

    def update_text_area(files = True, text = ""):
        text_area.config(state=tk.NORMAL)
        text_area.delete('1.0', tk.END)  # Clear the text area
        if files:
            for file_path in all_files:
                text_area.insert(tk.END, "/".join(file_path.split('/')[-2:]) + '\n')
        else: 
            text_area.insert(tk.END, text + '\n')
        text_area.config(state=tk.DISABLED)
        


    file_frame = CTkFrame(root, corner_radius=15, fg_color="#f0f0f0")
    file_frame.grid(row=0, column=0, padx=20, pady=20)

    select_button = CTkButton(file_frame, 
                              text="Select Files", 
                              command=select_files,
                              fg_color="#007bff",
                              hover_color="#0056b3",
                              text_color="#ffffff",
                              border_width=0)
    select_button.pack(pady=10)

    clear_button = CTkButton(file_frame, 
                              text="Clear Files", 
                              command=clear_files,
                              fg_color="#787878",
                              hover_color="#676767",
                              text_color="#ffffff",
                              border_width=0)
    clear_button.pack(pady=10)

    text_frame = CTkScrollableFrame(file_frame, width=530, height=200)
    text_frame.pack(pady=10)

    text_area = tk.Text(text_frame)
    text_area.pack(fill=tk.BOTH, expand=True)

    text_area.configure(wrap=None)

    read_parent_catalog_checkbox = CTkCheckBox(root,
                                                 text="Read Parent Catalog (from API)",
                                                 variable=read_parent_catalog_var,
                                                 onvalue=True,
                                                 offvalue=False,
                                                 fg_color="#007bff",
                                                 hover_color="#0056b3",
                                                 text_color="#000000")
    read_parent_catalog_checkbox.grid(row=1, column=0, pady=10)

    generate_test_node_checkbox = CTkCheckBox(root,
                                              text="Use Test STAC",
                                              variable=generate_test_node_var,
                                              onvalue=True,
                                              offvalue=False,
                                              fg_color="#007bff",
                                              hover_color="#0056b3",
                                              text_color="#000000")
    generate_test_node_checkbox.grid(row=2, column=0, pady=10)

    def execute_command(execute_button):
        if all_files:
        
            execute_button.configure(text=f"Generating collection(s) from {len(all_files)} file(s)")
            execute_button.update()
            base_command = [
                misc_config["python_path"],
                '"create_dynamic_catalog.py"'
            ]
            
            if read_parent_catalog_var.get():
                base_command.append('--readParentCatalog True')
            else:
                base_command.append('--readParentCatalog False')
            
            if generate_test_node_var.get():
                base_command.append('--testMode True')
            else:
                base_command.append('--testMode False')

            config_args = f'--configs {" ".join(all_files)}'
            full_command = ' '.join(base_command + [config_args])
            
            print(full_command)
            result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
            print(result.stdout)
            print(result.stderr)

            show_result_dialog(result.stdout, result.stderr)
            execute_button.configure(text="Generate")

        else:
            print("Please select at least one config file.")
            messagebox.showwarning("No Files Selected", "Please select at least one config file.")

    execute_button = CTkButton(root, text="Generate", command = lambda: execute_command(execute_button),
                               fg_color="#007bff",
                               hover_color="#0056b3",
                               text_color="#ffffff",
                               border_width=0)
    execute_button.grid(row=3, column=0, pady=20)

    root.mainloop()

def show_result_dialog(stdout, stderr):
    result_message = ""
    title = "Result"
    if stdout:
        result_message += "\n" + stdout + "\n\n"
    if stderr:
        title = "Error"
        result_message += "Error:\n" + stderr
    
    if not result_message:
        result_message = "Command executed successfully."
    
    messagebox.showinfo(title, result_message)

if __name__ == "__main__":
    main()
