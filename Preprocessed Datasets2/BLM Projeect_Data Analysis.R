#*===========================================================================================================*#
#*====== Nov 13, 2024 =======================================================================================*#
#*===========================================================================================================*#
#*====== Black CEO Project ========================================================================*#
#*===========================================================================================================*#


### DATA PREPARATION: PREPROCESSING ### 

# 필요한 패키지 로드
library(dplyr)

# CSV 파일 경로 설정
file1 <- "~/Desktop/Preprocessed Datasets2/1.firm ceo name only_cleaned.csv"
file2 <- "~/Desktop/Preprocessed Datasets2/3.BoardEx_processed2.csv"

# CSV 파일 읽기
df1 <- read.csv(file1, stringsAsFactors = FALSE)
df2 <- read.csv(file2, stringsAsFactors = FALSE)

# 'tic' 열을 기준으로 left join 수행
merged_df <- left_join(df1, df2, by = "tic")

# 병합된 데이터프레임 확인
head(merged_df)
View(merged_df)

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_df, "~/Desktop/Preprocessed Datasets2/1+3_R_merged.csv", row.names = FALSE)

#*===========================================================================================================*#
# 필요한 패키지 로드
library(dplyr)
library(readr)

# CSV 파일 읽기
file3 <- read_csv("~/Desktop/Preprocessed Datasets2/1+2_R_merged_manually checked.csv")
file4 <- read_csv("~/Desktop/Preprocessed Datasets2/2.Bermiss_unique gvkey_CEOonly.csv")

# 'gvkey'를 기준으로 left join 수행
merged_data <- left_join(file3, file4, by = "gvkey")

# 병합된 데이터 확인
print(merged_data)

# 병합된 데이터프레임 확인
head(merged_data)
View(merged_data)

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_data, "~/Desktop/Preprocessed Datasets2/1+2+3_R_merged.csv", row.names = FALSE)


#*===========================================================================================================*#
# 필요한 패키지 로드
library(dplyr)
library(readr)

# CSV 파일 읽기
file5 <- read_csv("~/Desktop/Preprocessed Datasets2/1+2+3_R_merged_manual checking.csv")
file6 <- read_csv("~/Desktop/Preprocessed Datasets2/Dataset_workfile_241107_CEO.csv")

# gvkey를 동일한 데이터 유형으로 변환 (숫자형으로 변환)
file5 <- file5 %>% mutate(gvkey = as.character(gvkey))
file6 <- file6 %>% mutate(gvkey = as.character(gvkey))


# 'gvkey'를 기준으로 left join 수행
merged_data2 <- left_join(file5, file6, by = "gvkey") ##이게안되니까

merged_data2 <- left_join(file5, file6, by = "gvkey", relationship = "many-to-many") ## 다대다 관계 처리


# 병합된 데이터 확인
print(merged_data2)

# 병합된 데이터프레임 확인
head(merged_data2)
View(merged_data2)

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_data2, "~/Desktop/Preprocessed Datasets2/1+2+3+workedfile_R_merged.csv", row.names = FALSE)


#*===========================================================================================================*#

# 필요한 패키지 로드
library(dplyr)
library(readr)

# CSV 파일 읽기
file7 <- read_csv("~/Desktop/Preprocessed Datasets2/new_merged_241113.csv")
file8 <- read_csv("~/Desktop/Preprocessed Datasets2/1+2+3_R_merged_manual.csv")

# '2First'와 '2Last' 열만 선택하여 'coid' 기준으로 병합
file8_subset <- file8 %>% select(coid, `2First`, `2Last`)

# 'coid'를 기준으로 left join 수행
merged_data <- left_join(file7, file8_subset, by = "coid", relationship = "many-to-many")

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_data, "~/Desktop/Preprocessed Datasets2/new_merged_241113_firstlastnameadd.csv", row.names = FALSE)

# 필요한 패키지 로드
library(dplyr)
library(readr)
library(stringr)  # stringr 패키지를 추가로 로드

# CSV 파일 읽기
file7 <- read_csv("~/Desktop/Preprocessed Datasets2/new_merged_241113.csv")

# 'ceo_name' 열을 'new_first'와 'new_last'로 분할
file7 <- file7 %>%
  mutate(new_first = word(ceo_name, 1),
         new_last = word(ceo_name, -1))

# 결과를 새로운 CSV 파일로 저장
write.csv(file7, "~/Desktop/Preprocessed Datasets2/new_merged_241113_with_firstlast.csv", row.names = FALSE)


------------------

  
# 필요한 패키지 로드
library(dplyr)
library(readr)

# CSV 파일 읽기
file9 <- read_csv("~/Desktop/Preprocessed Datasets2/new_merged_241113_with_firstlast.csv")
file10 <- read_csv("~/Desktop/Preprocessed Datasets2/2.Bermiss_unique gvkey_CEOonly.csv")
file11 <- read_csv("~/Desktop/Preprocessed Datasets2/2.Bermiss_cleaned.csv")

# 필요한 열(2BE, 2first)만 선택하고 'last' 열을 기준으로 left_join 수행
file11_selected <- file11 %>% select(last, `2BE`, `2first`)
merged_data <- left_join(file9, file11_selected, by = "last")

# 중복된 행 제거
merged_data <- merged_data %>% distinct()

# 결과를 새로운 CSV 파일로 저장
write.csv(merged_data, "~/Desktop/Preprocessed Datasets2/new_merged_241113_with_Bermisscleaned.csv", row.names = FALSE)
  
  
  
  


# 필요한 패키지 로드
library(dplyr)
library(readr)

# CSV 파일 읽기
file9 <- read_csv("~/Desktop/Preprocessed Datasets2/new_merged_241113_with_firstlast.csv")
file10 <- read_csv("~/Desktop/Preprocessed Datasets2/2.Bermiss_unique gvkey_CEOonly.csv")

# 'ceo_name'를 기준으로 left join 수행
merged_data3 <- left_join(file7, file8, by = "ceo_name", relationship = "many-to-many") ## 다대다 관계 처리

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_data2, "~/Desktop/Preprocessed Datasets2/new_merged_241113_firstlastnameadd.csv", row.names = FALSE)



file9 <- read_csv("~/Desktop/Preprocessed Datasets2/2.Bermiss_unique gvkey_CEOonly.csv")


# 'Lastname'를 기준으로 left join 수행
merged_data2 <- left_join(file5, file6, by = "gvkey") ##이게안되니까

merged_data2 <- left_join(file5, file6, by = "gvkey", relationship = "many-to-many") ## 다대다 관계 처리


# 병합된 데이터 확인
print(merged_data2)

# 병합된 데이터프레임 확인
head(merged_data2)
View(merged_data2)

# 병합된 데이터프레임을 새로운 CSV 파일로 저장
write.csv(merged_data2, "~/Desktop/Preprocessed Datasets2/1+2+3+workedfile_R_merged.csv", row.names = FALSE)

