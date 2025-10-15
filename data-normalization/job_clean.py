import pandas as pd
import numpy as np
import re

def parse_salary(s):
    if pd.isna(s):
        return np.nan, np.nan
    
    s = str(s).lower().strip()

    # Nếu là thỏa thuận
    if any(word in s for word in ['thỏa thuận', 'thoả thuận', 'negotiable']):
        return np.nan, np.nan

    # Bỏ ký tự rác
    s = s.replace(',', '').replace('~', ' ').replace('–', '-')

    # Tìm đơn vị
    is_usd = 'usd' in s
    is_vnd = 'triệu' in s or 'vnd' in s or '₫' in s

    # Lấy số
    nums = re.findall(r'\d+(?:\.\d+)?', s)
    if not nums:
        return np.nan, np.nan
    
    nums = [float(x) for x in nums]

    # Chỉ 1 giá trị -> gán min = max
    if len(nums) == 1:
        min_val, max_val = 0, nums[0]
    elif len(nums) == 2:
        min_val, max_val = nums[0], nums[-1]
    
    # Nếu đơn vị là triệu
    if is_vnd:
        min_val *= 1000000
        max_val *= 1000000
    elif is_usd:
        min_val *= 26000
        max_val *= 26000
    else:
        # Nếu không có đơn vị mà số nhỏ -> giả định triệu
        if max_val < 1000:
            min_val *= 1000000
            max_val *= 1000000
            
    if max_val == 0:
        max_val = np.nan
        min_val = np.nan

    return min_val, max_val

def clean_job_name(name):
        name = str(name).strip()
        name = re.sub(r'[–—]', '-', name) 
        # Cắt tại -, (, _ nếu phần trước có >4 từ
        parts = re.split(r'[-(_]', name)
        first_part = parts[0].strip()
        if len(first_part) > 4:
            name = first_part

        # Cắt dấu phẩy thông minh: chỉ cắt nếu sau dấu phẩy có chữ "lương", "salary", số, hoặc "đ"
        name = re.split(r',\s*(?=(lương|salary|\d|₫|upto))', name, flags=re.IGNORECASE)[0].strip()

        return name


def clean_salary_column(path_in, path_out):
    df = pd.read_csv(path_in, encoding='utf-8-sig')
    
    if 'Job_Name' in df.columns:
        df['Job_Name'] = df['Job_Name'].astype(str).apply(clean_job_name)
    
    if 'Required_Skills' in df.columns:
        df['Required_Skills'] = df['Required_Skills'].replace('', np.nan)
        df['Required_Skills'] = df['Required_Skills'].fillna('Không có')

    df[['min_salary', 'max_salary']] = df['Salary_Range'].apply(
        lambda x: pd.Series(parse_salary(x))
    )
    df.drop(columns=['Salary_Range'], inplace=True)
    df.drop(columns=['Updated_Time'], inplace=True)
    df.drop(columns=['Detail_Link'], inplace=True)
    
    min_min = df['min_salary'].min(skipna=True)
    min_max = df['max_salary'].min(skipna=True)

    df['min_salary'] = df['min_salary'].fillna(min_min)
    df['max_salary'] = df['max_salary'].fillna(min_max)
    
    df.to_csv(path_out, index=False, encoding='utf-8-sig')
    print(f"File cleaned saved at: {path_out}")

    return df

def test(df):
    data = pd.read_csv(df, encoding='utf-8-sig')
    data['Required_Skills'] = data['Required_Skills'].replace('Không có', np.nan)
    print(data['Required_Skills'].isna().sum())


if __name__ == "__main__":
    df_clean = clean_salary_column(r"D:\BigData\Project\Python\BigData\data\topcv_job_trends_all_2291_tins.csv", "topcv_job_trends_clean.csv")
    #test("topcv_job_trends_clean.csv")

