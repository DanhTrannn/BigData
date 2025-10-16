import pandas as pd
import numpy as np
import re

def parse_salary(s):
    if pd.isna(s):
        return np.nan, np.nan
    
    s = str(s).lower().strip()

    if any(word in s for word in ['thỏa thuận', 'thoả thuận', 'negotiable']):
        return np.nan, np.nan

    s = s.replace(',', '').replace('~', ' ').replace('–', '-')

    is_usd = 'usd' in s
    is_vnd = 'triệu' in s or 'vnd' in s or '₫' in s

    nums = re.findall(r'\d+(?:\.\d+)?', s)
    if not nums:
        return np.nan, np.nan
    
    nums = [float(x) for x in nums]

    if len(nums) == 1:
        min_val, max_val = 0, nums[0]
    else:
        min_val, max_val = nums[0], nums[-1]
    
    if is_vnd:
        min_val *= 1_000_000
        max_val *= 1_000_000
    elif is_usd:
        min_val *= 26_000
        max_val *= 26_000
    else:
        if max_val < 1000:
            min_val *= 1_000_000
            max_val *= 1_000_000

    if max_val == 0:
        return np.nan, np.nan

    return min_val, max_val

def clean_job_name(name):
    name = str(name).strip()
    name = re.sub(r'[–—]', '-', name)
    parts = re.split(r'[-(_]', name)
    first_part = parts[0].strip()
    if len(first_part) > 4:
        name = first_part
    name = re.split(r',\s*(?=(lương|salary|\d|₫|upto))', name, flags=re.IGNORECASE)[0].strip()
    return name

def normalize_required_skills(skills):
    if pd.isna(skills):
        return np.nan

    s = str(skills).strip()
    if s.lower() in ['không có', 'none', 'nan', '', 'no', 'null']:
        return np.nan

    s = s.replace('"', '').replace("'", '')
    s = re.sub(r'[\n\r\t]', ' ', s)
    s = re.sub(r'\s*,\s*', '|', s)
    s = re.sub(r'[;/•\-–—]+', '|', s)
    s = re.sub(r'\s*\|\s*', '|', s)
    s = re.sub(r'\s{2,}', ' ', s)

    skills_list = [w.strip().title() for w in s.split('|') if w.strip()]
    return '|'.join(skills_list) if skills_list else np.nan

def normalize_job_data(path_in, path_out):
    df = pd.read_csv(path_in, encoding='utf-8-sig')

    # --- Làm sạch Job_Name
    if 'Job_Name' in df.columns:
        df['Job_Name'] = df['Job_Name'].astype(str).apply(clean_job_name)

    #--- Làm sạch Required_Skills
    if 'Required_Skills' in df.columns:
        df['Required_Skills'] = df['Required_Skills'].astype(str).apply(normalize_required_skills)
        df['Required_Skills'] = df['Required_Skills'].fillna('Không có')

    # --- Làm sạch Salary
    if 'Salary_Range' in df.columns:
        df[['min_salary', 'max_salary']] = df['Salary_Range'].apply(lambda x: pd.Series(parse_salary(x)))
        df.drop(columns=['Salary_Range'], inplace=True, errors='ignore')

    # --- Loại bỏ các cột không cần thiết nếu có
    df.drop(columns=['Updated_Time', 'Detail_Link'], inplace=True, errors='ignore')

    # --- Bổ sung giá trị thiếu
    if 'min_salary' in df.columns and 'max_salary' in df.columns:
        df['min_salary'].fillna(df['min_salary'].min(skipna=True), inplace=True)
        df['max_salary'].fillna(df['max_salary'].min(skipna=True), inplace=True)

    df.to_csv(path_out, index=False, encoding='utf-8-sig')
    print(f"File cleaned saved at: {path_out}")
    return df

if __name__ == "__main__":
    input_path = r"clean-data\topcv_job_trends_clean.csv"
    output_path = r"clean-data\topcv_job_trends_cleaned.csv"

    normalize_job_data(input_path, output_path)