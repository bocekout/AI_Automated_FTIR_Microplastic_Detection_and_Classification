# module: ingest_ir
import pandas as pd
import numpy as np
import glob
import os

def ingest_file(filepath, material=None, drop_columns=None, drop_rows=None, reading_format='vertical', num_spectra=1):
    
    # Detect file extension
    file_type = os.path.splitext(filepath)[-1].lower()
    print(f'file type: {file_type}')

    # Read in the file
    if file_type in ['.csv','.tsv','.txt']:
        spectrum_df = pd.read_csv(filepath, header=None, skip_blank_lines=True)
        print(".csv, .tsv, or .txt file detected")
    elif file_type == '.xlsx':
        spectrum_df = pd.read_excel(filepath, header=None, skip_blank_lines=True)
        print(".xlsx file detected - please note that only the first sheet will be ingested.")
    else:
        return "Current supported filetypes are: '.csv', '.tsv', '.txt', and '.xlsx'"
    
    # Drop extraneous rows or columns 
    if drop_columns is not None:
        spectrum_df = spectrum_df.drop(columns=drop_columns) # Since we excluded headers, column names are integers so drop_columns should be a list or range of integers
    elif drop_rows is not None:
        spectrum_df = spectrum_df.drop(index=drop_rows) 
    else:
        pass

    # Reset column names and index
    spectrum_df.columns = list(range(len(spectrum_df.columns)))
    spectrum_df.index = list(range(len(spectrum_df.index)))

    # Transpose data if necessary - after transposition material labels should be row zero and wavenumbers should be column zero.
    if reading_format == 'horizontal':
        spectrum_df = spectrum_df.T
        print("data transposed to vertical format")
    elif reading_format == 'vertical':
        print("vertical format given")
    else:
        print("Please pass a valid reading_format: 'horizontal' if one material corresponds to one row, 'vertical' if one material corresponds to one column")

    # Check if single or multiple spectra to get or set material_label accordingly
    if num_spectra == 1:
        if material == None:
            material_label = [os.path.basename(filepath)]
        elif isinstance(material, str):
            material_label=[material]
        else:
            raise Exception("For single spectrum, material should either be None or a string label. File name is used by default.")

    elif num_spectra > 1 & isinstance(num_spectra, int):
        if material == None:
            material_label = [spectrum_df.iloc[0]][1:]
        elif isinstance(material, list):
            material_label = material
        else:
            raise Exception("""For multiple spectra, material should either be None or a list of strings. \n 
                            After dropping rows/columns, the first remaining row/column is used by default, depending on reading_format.""")
    
    else:
        raise Exception("Num_spectra must be integer greater than or equal to 1.")

    # Wavenumber should be zero column after dropping rows/columns and transposing if necessary. 
    wavenumber_scaled_4k = list(list(pd.to_numeric(spectrum_df[0]), errors='coerce')[1:]/4000)
    spectrum_df = spectrum_df.drop(columns=0)

    # sanity check on wavenumbers
    if np.min(wavenumber_scaled_4k) <= 395/4000 or np.max(wavenumber_scaled_4k) >= 4005/4000:
        raise Exception('Detected wavenumbers out of range. Please check drop_columns, drop_rows, and reading_format. Acceptable wavenumbers range from 395 to 4005')
    elif len(wavenumber_scaled_4k) > 4000:
        raise Exception('Spectral resolution too high. Please input fewer than 4000 datapoints per spectrum.')
    else:
        pass

    # Now that wavenumber list is known we can build our pad to make sure the tuple lists are always 4000 long.
    padding_length = 4000 - len(wavenumber_scaled_4k)
    padding = [tuple([0,0]) for i in range(padding_length)]
    print(f"Number readings: {len(wavenumber_scaled_4k)}")
    print(f"Padding length: {len(padding)}")

    # Convert remaining columns to numeric
    for col in spectrum_df.columns:
        spectrum_df[col] = pd.to_numeric(spectrum_df[col], errors='coerce')
    
    # Fill missing values
    spectrum_df = spectrum_df.fillna(0)
    # Reset columns again
    spectrum_df.columns = list(range(len(spectrum_df.columns)))

    # Scale down from %T or %A to just T or A
    if np.max(spectrum_df) > 2:
        spectrum_df = spectrum_df/100 
        print('Percentage detected and converted')
    else:
        pass
    
    # Convert T to A
    if np.mean(spectrum_df[0]) > 0.5:
        spectrum_df = 1 - spectrum_df 
        print('Transmission detected, converted to Absorbance')
    else:
        print('Absorbance detected, no conversion needed')

    # Build the output array - labels + padded lists of tuples
    first=list(spectrum_df[0])
    first_zipped = list(zip(wavenumber_scaled_4k, first))
    print(first_zipped)
    first_zipped_padded = first_zipped + padding
    print(f"Padded spectral length: {len(first_zipped_padded)}")
    print(f"Readings: {len(wavenumber_scaled_4k)}, datapoints: {len(first)}")
    output = [material_label[0],first_zipped_padded]

    i = 1
    while i < len(spectrum_df.columns):
        spectrum = list(zip(wavenumber_scaled_4k, list(spectrum_df[i]))) + padding
        spec = [material_label[i],spectrum]
        output = np.vstack([output, spec])
        i += 1  

    return output


def ingest_folder(folderpath, reading_format='vertical', num_spectra=1, drop_columns=None, drop_rows=None, material=None):
    file_list = glob.glob(folderpath)
    i=1
    if material == None:
        output = ingest_file(file_list[0], reading_format=reading_format, num_spectra=num_spectra, drop_columns=drop_columns, drop_rows=drop_rows)
        while i < len(file_list):
            
            output = output.vstack(ingest_file(file_list[i], reading_format=reading_format, num_spectra=num_spectra, drop_columns=drop_columns, drop_rows=drop_rows))
            i+=1

        return output
    
    elif isinstance(material, list) & len(material) == len(file_list):
        output = ingest_file(file_list[0], material=material[0], reading_format=reading_format, num_spectra=num_spectra, drop_columns=drop_columns, drop_rows=drop_rows)
        while i < len(file_list):
            ingest_file(file_list[i], material=material[i], reading_format=reading_format, num_spectra=num_spectra, drop_columns=drop_columns, drop_rows=drop_rows)
            i+=1

        return output   
    
    else:
        raise Exception("""Parameter 'material' should either be list with length matching number of files in folder or none. 
                        If num_spectra > 1, material list should contain lists matching num_spectra, otherwise a list of strings is appropriate.""")