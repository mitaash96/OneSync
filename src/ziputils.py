import os
import zipfile
import shutil
import tempfile


def add_files_to_zip(zipfile_path, files_to_add):
    
    if not os.path.exists(zipfile_path):
        with zipfile.ZipFile(zipfile_path, 'w') as empty_file:
            pass

    with zipfile.ZipFile(zipfile_path, 'a') as existing_zip:
        for file_path in files_to_add:
            file_name = os.path.basename(file_path)
            existing_zip.write(file_path, arcname=file_name)
            os.remove(file_path)


def extract_and_remove_from_zip(zipfile_path, files_n_destinations):
    
     with tempfile.TemporaryDirectory() as temp_dir:
        # Extract the entire ZIP file to the temporary directory
        with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Path to the extracted file in the temporary directory
        extracted_file_paths = dict(
            zip(
                list(map(lambda x: os.path.join(temp_dir, x), list(files_n_destinations.keys()))),
                list(files_n_destinations.values())
                )
            )
        
        for f, destination_folder in extracted_file_paths.items():
        # Check if the file exists in the temporary directory
            if os.path.exists(f):
                # Move the file to the destination folder
                shutil.move(f, destination_folder)
                print(f"File '{f}' extracted to '{destination_folder}'")
            
            else:
                print(f"File '{f}' not found in the ZIP archive.")

        if len(os.listdir(temp_dir))>0:
            with zipfile.ZipFile(zipfile_path, 'w') as new_zip:
                # Add the remaining files in the temporary directory to the new ZIP file
                for root, _, files in os.walk(temp_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        relative_path = os.path.relpath(file_path, temp_dir)
                        new_zip.write(file_path, arcname=relative_path)
                    
            print(f"Remaining files in '{temp_dir}' added to the ZIP file.")
        else:
            os.remove(zipfile_path)



if __name__ == '__main__':
    pass
