import os
import requests
import zipfile

def download_silesia(dest_folder="silesia_corpus"):
    # Primary URL for the Silesia Corpus
    url = "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip"
    
    # Create the directory if it doesn't exist
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
    zip_path = os.path.join(dest_folder, "silesia.zip")
    
    print("--- Starting Download ---")
    print(f"Target: {url}")
    
    try:
        # Stream the download to handle the ~60MB zip file efficiently
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024): # 1MB chunks
                if chunk:
                    f.write(chunk)
        
        print(f"Download complete. Extracting files to '{dest_folder}'...")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(dest_folder)
            
        # Optional: Remove the zip file after extraction to save space
        os.remove(zip_path)
        
        print(f"--- Success! ---")
        print(f"Files extracted to: {os.path.abspath(dest_folder)}")
        print(f"Total files: {len(os.listdir(dest_folder))}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip archive.")

if __name__ == "__main__":
    download_silesia()