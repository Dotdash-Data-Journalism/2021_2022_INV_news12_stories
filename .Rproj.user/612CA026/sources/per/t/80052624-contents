library(rlang)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(tibble)
library(readxl)
library(stringi)
library(lubridate)
library(janitor)
library(rjson)
library(xml2)
library(rvest)
library(zoo)
library(magrittr)
library(googlesheets4)
library(httr)


weekOfAnalysisDate <- list.files("./visualizations/") %>% 
  str_match_all("\\d{4}-\\d{2}-\\d{2}_.*") %>%
  compact() %>% 
  str_extract_all("\\d{4}-\\d{2}-\\d{2}") %>% 
  flatten_chr() %>% 
  base::as.Date() %>% 
  sort(decreasing = T) %>% 
  nth(1) + 7

### Configure googlesheet4 auth

paste0("{
\"type\": \"service_account\",
  \"project_id\": \"inv-ny1-covid-recovery-index\",
  \"private_key_id\": \"", Sys.getenv("PRIVATE_KEY_ID"), "\",
  \"private_key\": \"", Sys.getenv("PRIVATE_KEY"), "\",
  \"client_email\": \"", Sys.getenv("CLIENT_EMAIL"), "\",
  \"client_id\": \"", Sys.getenv("PRIVATE_KEY_ID"), "\",
  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",
  \"token_uri\": \"https://oauth2.googleapis.com/token\",
  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",
  \"client_x509_cert_url\": \"", Sys.getenv("CLIENT_CERT_URL"), "\"
}") -> google_service_account


gs4_auth(path = google_service_account)

### Open Table Data
## From section: https://www.opentable.com/state-of-industry
## Seated diners from online, phone, and walk-in reservations

Sys.sleep(3)
otUpdate <- tryCatch({

  otScripts <- read_html("https://www.opentable.com/state-of-industry") %>%
    html_nodes("script")

  otDataScript <- which(map_lgl(map_chr(otScripts, html_text), ~str_detect(.x, "__INITIAL_STATE__")))

  otRawText <- otScripts %>%
    nth(otDataScript) %>%
    html_text()

  Sys.sleep(5)

  otRawJSON <- otRawText %>%
    str_match(regex("w.__INITIAL_STATE__ =\\s+(.*)", dotall = T)) %>%
    nth(2) %>%
    str_remove(";\\}\\)\\(window\\);$")


  Sys.sleep(5)

  otData <- otRawJSON %>%
    fromJSON(json_str = .)


  otChrDates <- otData %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("headers")

  otDates <- lubridate::ymd(otChrDates) %>% sort()

  otYoY <- otData %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("cities") %>%
    map_dfr(., ~tibble(
      pct_chg = .$yoy,
      city = .$name
    )) %>%
    filter(city == "New York")

  otOld <- read_csv("./data/opentable.csv",
                    col_types = "iDddd")

  openTableReadyNew <- otYoY %>%
    mutate(Date = otDates, prePanChg = rollmean(pct_chg, k = 7, fill = NA, align = "right")) %>%
    select(-pct_chg) %>%
    pivot_wider(names_from = "city", values_from = "prePanChg") %>%
    rename(`7-day Average` = `New York`) %>%
    mutate(`Day of Week` = wday(Date),
           `OpenTable YoY Seated Diner Data (%)` = NA_integer_,
           `Restaurant Reservations Index` = `7-day Average` + 100) %>%
    relocate(`Day of Week`, .before = Date) %>%
    filter(Date > max(otOld$Date) & Date <= weekOfAnalysisDate) %>%
    arrange(desc(Date))

  openTableReady <- bind_rows(openTableReadyNew, otOld)

},
error = function(e) {
  eFull <- error_cnd(class = "openTableError", message = paste("An error occured with the Open Table update:",
                                                               e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
}
)

Sys.sleep(3)

### MTA ridership
Sys.sleep(3)
mtaUpdate <- tryCatch(
  {

    mtaRidershipNYC <- read_csv("https://new.mta.info/document/20441", col_types = "cicicicicicic") %>%
      select(Date, `Subways: Total Estimated Ridership`, `Subways: % of Comparable Pre-Pandemic Day`) %>%
      mutate(`Subways: % of Comparable Pre-Pandemic Day` = (as.double(str_remove_all(`Subways: % of Comparable Pre-Pandemic Day`, "%")) - 100) / 100,
             Date = mdy(Date)) %>%
      arrange(Date) %>%
      mutate(`7-day Average` = rollmean(`Subways: % of Comparable Pre-Pandemic Day`, k = 7, fill = NA, align = "right"),
             `Subway Mobility Index` = (1 + `7-day Average`) * 100) %>%
      mutate(`Day of Week` = c(7, rep_len(seq(1, 7, 1), nrow(.) - 1)),
             `Avg. Ridership` = rollmean(`Subways: Total Estimated Ridership`, k = 7, fill = NA, align = "right")) %>%
      relocate(`Day of Week`, .before = Date) %>%
      select(-`Subways: Total Estimated Ridership`) %>%
      arrange(desc(Date)) %>% 
      filter(Date <= weekOfAnalysisDate)

  },
  error = function(e) {
    eFull <- error_cnd(class = "mtaError", message = paste("An error occured with the MTA update:",
                                                           e, "on", Sys.Date(), "\n"))

    write(eFull[["message"]], "./errorLog.txt", append = T)
    message(eFull[["message"]])

    return(eFull)
  }
)


Sys.sleep(3)
### UI Data
uiUpdate <- tryCatch({


  ### Citywide aggregated from Weekly County Claims
  download.file(url = "https://dol.ny.gov/statistics-weekly-claims-and-benefits-report",
                destfile = "./data/nyUIClaimsWeekly.xlsx",
                method = "libcurl")

  read_excel(path = "./data/nyUIClaimsWeekly.xlsx",
             sheet = "Weekly Initial Claims by County",
             skip = 1) %>%
    pivot_longer(!c(Region, County), names_to = "Date", values_to = "Claims") %>%
    rename_with(.fn = make_clean_names, .cols = everything()) %>%
    mutate(date = base::as.Date(date, format = "%B %d, %Y"),
           claims = as.integer(claims),
           isoweek = isoweek(date)) %>%
    group_by(region, date, isoweek) %>%
    summarize(total_claims = sum(claims, na.rm = T)) %>%
    ungroup() %>%
    filter(region == "New York City", date >= base::as.Date("2020-01-01")) -> latest_claims


  read_csv("./data/ui_claims_2019.csv", col_names = T,
           col_types = "Diid") -> claims2019

  latest_claims %>%
    left_join(claims2019, by = "isoweek") %>%
    mutate(pct_change_19_now = (total_claims - rolling_avg_claims) / rolling_avg_claims,
           `Unemployment Claims Index` = 100 / ((100 * pct_change_19_now) + 100) * 100) %>%
    arrange(desc(date)) -> updatedNYCUI


},
error = function(e) {
  eFull <- error_cnd(class = "uiError", message = paste("An error occured with the UI update:",
                                                        e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})


### Covid-19 Data
Sys.sleep(3)
covidUpdate <- tryCatch({

  newNYCCovid19Hospitalizations <- read_csv("https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv",
                                            col_types = cols(.default = col_character()), col_names = T) %>%
    select(date_of_interest, HOSPITALIZED_COUNT) %>%
    mutate(date_of_interest = mdy(date_of_interest),
           HOSPITALIZED_COUNT = as.integer(HOSPITALIZED_COUNT),
           rolling_seven = rollmean(HOSPITALIZED_COUNT, k = 7, fill = NA, align = "right"),
           log_hosp = log10(rolling_seven + 1),
           `Covid-19 Hospitalizations Index` = (1 - log_hosp / 3.5) * 100) %>%
    filter(!is.na(`Covid-19 Hospitalizations Index`), date_of_interest <= weekOfAnalysisDate) %>%
    arrange(desc(date_of_interest))

  fullNYCCovid19Hospitalizations <- read_csv("./data/nyc_covid19_hospitalizations.csv",
                                             col_types = "Diddd")

  newNYCCovid19Hospitalizations <- newNYCCovid19Hospitalizations %>%
    filter(date_of_interest > max(fullNYCCovid19Hospitalizations$date_of_interest))

  updatedNYCCovid19Hospitalizations <- bind_rows(newNYCCovid19Hospitalizations, fullNYCCovid19Hospitalizations)

},
error = function(e) {
  eFull <- error_cnd(class = "covidError", message = paste("An error occured with the Covid update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

### Home Sales Street Easy
Sys.sleep(3)
homeSalesUpdate <- tryCatch({

  streetEasyLatest <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                 sheet = "City Wide Data",
                                 col_types = "Diiii") %>%
    mutate(iso_week = isoweek(`Week Ending`)) %>%
    filter(between(`Week Ending`, left = as.Date("2020-01-01"), right = (weekOfAnalysisDate + 1))) %>%
    select(`Week Ending`, `Number of Pending Sales`, iso_week)

  # read_excel(path = "./data/StreetEasy -- Dot Dash Weekly Data.xlsx",
  #            sheet = "City Wide Data",
  #            col_names = T,
  #            col_types = c("date", "numeric", "numeric", "numeric", "numeric")) %>%
  #   mutate(`Week Ending` = base::as.Date(`Week Ending`),
  #          iso_week = isoweek(`Week Ending`)) %>%
  #   filter(between(`Week Ending`, left = as.Date("2020-01-01"), right = (weekOfAnalysisDate + 1))) %>%
  #   select(`Week Ending`, `Number of Pending Sales`, iso_week) -> streetEasyLatest

  streetEasyRef <- read_csv("./data/streeteasy_home_sales_ref.csv", col_names = T,
                            col_types = "dd")

  streetEasyLatest %>%
    inner_join(streetEasyRef, by = "iso_week") %>%
    mutate(`Home Sales Index` = (`Number of Pending Sales` / three_week_2019_rolling_average) * 100,
           Date = (`Week Ending` - 1)) %>%
    arrange(desc(`Week Ending`)) -> streetEasyFull


},
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

Sys.sleep(5)
rentalsUpdate <- tryCatch({

  nycRentalsFull <- read_csv("./data/streeteasy_rentals.csv",
                             col_types = "iDdiddd")

  latestNYCRental <- read_sheet(ss = "17v5PF6LqZLbNEhq2BPKarmukR_hxn7lXXq27weXSXxk",
                                sheet = "City Wide Data",
                                col_types = "Diiii") %>%
    filter(`Week Ending` == weekOfAnalysisDate + 1) %>%
    select(`Rental Inventory`) %>%
    pull()

  # read_excel(path = "./data/StreetEasy -- Dot Dash Weekly Data.xlsx",
  #            sheet = "City Wide Data",
  #            col_names = T,
  #            col_types = c("date", "numeric", "numeric", "numeric", "numeric")) %>%
  #   mutate(`Week Ending` = base::as.Date(`Week Ending`)) %>%
  #   filter(`Week Ending` == weekOfAnalysisDate + 1) %>%
  #   select(`Rental Inventory`) %>%
  #   pull() -> latestNYCRental

  newNYCRentals <- tibble_row(
    `Week of Year` = isoweek(weekOfAnalysisDate),
    `Week Ending` = weekOfAnalysisDate,
    `10-yr Median Model` = case_when(month(weekOfAnalysisDate) == 1 ~ 0.9891,
                                     month(weekOfAnalysisDate) == 2 ~ 0.9811,
                                     month(weekOfAnalysisDate) == 3 ~ 1.0659,
                                     month(weekOfAnalysisDate) == 4 ~ 1.0775,
                                     month(weekOfAnalysisDate) == 5 ~ 1.1359,
                                     month(weekOfAnalysisDate) == 6 ~ 1.1893,
                                     month(weekOfAnalysisDate) == 7 ~ 1.2042,
                                     month(weekOfAnalysisDate) == 8 ~ 1.1728,
                                     month(weekOfAnalysisDate) == 9 ~ 1.0496,
                                     month(weekOfAnalysisDate) == 10 ~ 1.0406,
                                     month(weekOfAnalysisDate) == 11 ~ 0.9774,
                                     month(weekOfAnalysisDate) == 12 ~ 0.8810),
    `Rental Inventory` = latestNYCRental,
    `Indexed to January Average` = (latestNYCRental - latestNYCRental * (1.11108297 - 1) * isoweek(weekOfAnalysisDate) / 52.28571429) / 16366.8,
    Difference = abs(`Indexed to January Average`/`10-yr Median Model`-1),
    `Rental Inventory Index` = 100 / (Difference + 1)
  )

  nycRentalsUpdated <- bind_rows(newNYCRentals, nycRentalsFull)

},
error = function(e) {
  eFull <- error_cnd(class = "streetEasy", message = paste("An error occured with the streetEasy update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

Sys.sleep(3)
dataFileUpdate <- tryCatch({
  latestWeeks <- streetEasyFull %>%
    inner_join(nycRentalsUpdated, by = c("Date" = "Week Ending")) %>%
    inner_join(updatedNYCCovid19Hospitalizations, by = c("Date" = "date_of_interest")) %>%
    inner_join(updatedNYCUI, by = c("Date" = "date")) %>%
    inner_join(openTableReady, by = "Date") %>%
    inner_join(mtaRidershipNYC, by = c("Date")) %>%
    select(Date, `Covid-19 Hospitalizations Index`, `Unemployment Claims Index`,
           `Home Sales Index`, `Rental Inventory Index`, `Subway Mobility Index`,
           `Restaurant Reservations Index`) %>%
    filter(Date %in% c(weekOfAnalysisDate, weekOfAnalysisDate - 7)) %>%
    arrange(Date)


  WoWChange <- map_dfr(latestWeeks[2:7], diff) %>%
    mutate(`New York City Recovery Index` = sum(
      map_dbl(
        select(
          filter(
            latestWeeks, Date == weekOfAnalysisDate
          ), -Date
        ), ~.x/6
      )) - sum(
        map_dbl(
          select(
            filter(
              latestWeeks, Date == weekOfAnalysisDate - 7
            ), -Date
          ), ~.x/6
        )
      )
    ) %>%
    relocate(`New York City Recovery Index`, everything()) %>%
    pivot_longer(everything(), names_to = "DATE", values_to = as.character(weekOfAnalysisDate))


  indexRecoveryOverview <- read_csv("./data/nyc_recovery_index_overview.csv",
                                    col_types = "Ddddddd")


  latestNYRIScores <- latestWeeks %>%
    filter(Date == weekOfAnalysisDate) %>%
    mutate(across(2:7, ~. / 6))


  indexRecoveryOverviewLatest <- bind_rows(latestNYRIScores, indexRecoveryOverview)

},
error = function(e) {
  eFull <- error_cnd(class = "dataUpdate", message = paste("An error occured with the data writing update:",
                                                           e, "on", Sys.Date(), "\n"))

  write(eFull[["message"]], "./errorLog.txt", append = T)
  message(eFull[["message"]])

  return(eFull)
})

if (any(map_lgl(list(otUpdate, mtaUpdate, uiUpdate, covidUpdate,
                     homeSalesUpdate, rentalsUpdate, dataFileUpdate
), ~class(.x)[2] == "rlang_error"), na.rm = T)) {
  stop("There was an error in the update, check the error log to see more.")
} else {
  print("Data update was successful! Writing files and pushing to Git...")
  write_csv(openTableReady, "./data/opentable.csv")
  Sys.sleep(2)
  write_csv(mtaRidershipNYC, "./data/mta_subway.csv")
  Sys.sleep(2)
  write_csv(updatedNYCUI, "./data/nyc_ui.csv")
  Sys.sleep(2)
  write_csv(updatedNYCCovid19Hospitalizations, "./data/nyc_covid19_hospitalizations.csv")
  Sys.sleep(2)
  write_csv(streetEasyFull, "./data/streeteasy_home_sales.csv")
  Sys.sleep(2)
  write_csv(nycRentalsUpdated, "./data/streeteasy_rentals.csv")
  Sys.sleep(2)
  write_csv(WoWChange, paste0("./visualizations/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_WoW_Changes.csv"))
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, paste0("./visualizations/", weekOfAnalysisDate, "_DW_NYC_Recovery_Index_Overview.csv"))
  Sys.sleep(2)
  write_csv(indexRecoveryOverviewLatest, "./data/nyc_recovery_index_overview.csv")
}

