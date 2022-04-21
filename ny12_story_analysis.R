library(readr)
library(dplyr)
library(rlang)
library(stringr)
library(tidyr)
library(lubridate)
library(xml2)
library(rvest)
library(purrr)
library(rjson)
library(magrittr)
library(janitor)
library(readxl)
library(ggplot2)
library(scales)
library(ggrepel)
library(zoo)
library(httr)

### Reading in reference files ###
# Fips Code table
fips_codes <- read_csv(file = "./reference_files/fips_codes_table.csv", 
                      col_types = "ccc") %>% 
  mutate(fips = if_else(nchar(fips) < 5, paste0("0", fips), fips),
         county = case_when(county == "New York" ~ "Manhattan",
                            county == "Richmond" & state == "NY" ~ "Staten Island",
                            county == "Kings" & state == "NY" ~ "Brooklyn",
                          T ~ county))

# Counties News12 cares about
news_twelve_tri_state <- fips_codes %>% 
  filter((county %in% c("Westchester", "Queens", "Brooklyn", 
                      "Bronx", "Suffolk", "Nassau", "Staten Island", "Manhattan") & state == "NY")
         | (county %in% c("Bergen", "Hudson", "Morris", "Monmouth") & state == "NJ")
         | (county %in% c("Fairfield", "New Haven", "Litchfield", "Hartford") & state == "CT")) %>% 
  pull(fips)

# NYC counties
nyc_county_fips <- fips_codes %>% 
  filter((county %in% c("Queens", "Brooklyn", 
                      "Bronx", "Staten Island", "Manhattan") & state == "NY")) %>% 
  pull(fips)

# News12 counties minus NYC
news_twelve_tri_state_no_nyc <- news_twelve_tri_state[!(
  news_twelve_tri_state %in% nyc_county_fips
  )]

# Time frames for bar graphs
time_frames <- list("Month-over-month", "Year-over-year", "2019 to present")

### Socrata password ##
SCT_PW <- Sys.getenv("SCT_PW")

### Captions ###
## Manipulation add-ons ##
ts_caption <- "Data reflects trailing seven-day average."
agg_caption <- "Data reflects monthly average of percent changes."
inf_caption <- "Dollars inflation adjusted to most recent dollar value with BLS New York-Newark-Jersey City, NY-NJ-PA MSA CPI for all items."

## Opportunity insights citation ##
oi_addition <- paste0("\n\"The Economic Impacts of COVID-19: Evidence from a New Public Database Built Using Private Sector Data\",\n",
                      "by Raj Chetty, John Friedman, Nathaniel Hendren, Michael Stepner, and the Opportunity Insights Team.\n",
                      "November 2020. Available at: https://opportunityinsights.org/wp-content/uploads/2020/05/tracker_paper.pdf")

## Specific measure change explanations ##
ot_change <- paste0("Percent change is change in seated diners from online, phone, and walk-in reservations\n",
                    "from the same day of the same week in 2020-present to the same day of the same week in 2019 (not the same date)\n")

consumer_spending_oi <- "Percent change is seasonally adjusted change since January 2020.\n"
mobility_oi <- "Percent change is change relative to the January 2020 index period, not seasonally adjusted.\n"
cpi_ne_addition <- "\nData is for New England Census Division and includes CT, RI, MA, VT, NH, and ME"
cpi_nycmsa_addition <- paste0("\nData is for the New York-Newark-Jersey City Metro Statistical Area (MSA) and includes\n",
                              "NYC, Westchester, Suffolk, Nassau, Putnam, and Rockland counties in NY.\n",
                              "Bergen, Hudson, Passaic, Middlesex, Monmouth, Ocean, Somerset, Essex,\n",
                              "Union, Morris, Sussex, and Hunterdon counties in NJ and Pike county in PA."
                              )


## Full measure captions
bls_caption <- "Source: Bureau of Labor Statistics."
bls_cpi_ne <- paste0(bls_caption,
                     cpi_ne_addition)
bls_cpi_nycmsa <- paste0(bls_caption,
                         cpi_nycmsa_addition)
mta_caption <- "Source: Metropolitan Transporation Authority"
mta_monthly_prepan <- paste0("Source: Metropolitan Transporation Authority.\n",
                             "Comparison is for latest 12 months of data available for each transportation method")
home_prices_caption <- "Source: Zillow Home Value Index (ZHVI)"
rent_cost_caption <- "Source: Apartment List Data & Rent Estimates"
gas_price_caption <- "Source: Energy Information Administration"
ot_stem <- "Source: OpenTable. "
mobility_stem <- paste0("Source: Google LLC \"Google COVID-19 Community Mobility Reports\".\nhttps://www.google.com/covid19/mobility/ Accessed: ",
                        Sys.Date(), ". via Opportunity Insights\n")
consumer_spending_stem <- "Source: Affinity Solutions via Opportunity Insights\n"

ot_ts_caption <- paste0(ot_stem,
                        ot_change,
                        ts_caption)
ot_agg_caption <- paste0(ot_stem,
                         ot_change,
                         agg_caption)

mobility_ts_caption <- paste0(
  mobility_stem,
  mobility_oi,
  ts_caption,
  oi_addition)

mobility_agg_caption <- paste0(
  mobility_stem,
  mobility_oi,
  agg_caption,
  oi_addition
)

consumer_spending_ts_caption <- paste0(
  consumer_spending_stem,
  consumer_spending_oi,
  ts_caption,
  oi_addition
)

consumer_spending_agg_caption <- paste0(
  consumer_spending_stem,
  consumer_spending_oi,
  agg_caption,
  oi_addition
)

### Functions needed ###
# Function to clean LAUS data
cleaning_laus <- function(df_county, df_city, measure, area_df) {
  # County
  df_county %>% 
    filter(str_sub(series_id, 19, 20) == measure) -> df_no_label_county
  
  # City
  df_city %>% 
    filter(str_sub(series_id, 19, 20) == measure) -> df_no_label_city
  
  # Making county file
  laus_county_df <- df_no_label_county %>% 
    inner_join(area_df, by = "area_code") %>% 
    select(series_id, date, fips_code, value, area_text) %>% 
    separate(col = area_text, into = c("county", "state"), sep = ", ") %>% 
    mutate(county = str_remove(county, "\\s+County"))
  
  # Making city file
  laus_city_df <- df_no_label_city %>% 
    inner_join(area_df, by = "area_code") %>% 
    select(series_id, date, fips_code, value, area_text) %>% 
    separate(col = area_text, into = c("county", "state"), sep = ", ") %>% 
    mutate(county = str_to_title(county))
  
  # Making combo file
  if (measure == "03") {
    laus_tri_state_full <- bind_rows(
      laus_county_df, laus_city_df
    ) %>% 
      rename(pct = value)
  } else {
    laus_tri_state_full <- bind_rows(
      laus_county_df, laus_city_df
    )
  }
  
  # Finding latest most common date
  laus_tri_state_full %>% 
    group_split(state) %>% 
    map_dbl(~max(.x$date)) %>% 
    min() %>% 
    base::as.Date(origin = "1970-01-01") -> max_combo_date
  
  laus_list <- list(laus_tri_state_full, max_combo_date)
  
  return(laus_list)
  
}

# Function to make WoW, MoM, YoY, and '19 to present changes
change_time_bar_calc <- function(df, time_change, level, weekly = F) {
  
  level_name <- sym(level)
  
  if (weekly) {
    df_week <- df %>%
      mutate(year = year(date), week = week(date))
    
    latest_week_year <- max(filter(df_week, year == max(year))$week)
    max_week_num_prev_year <- max(filter(df_week, year == (max(year) - 1))$week)
    
    # Getting WoW latest year + week and year + week of week prior
    if (latest_week_year == 1) {
      latest_year_wow_prev <- max(df_week$year) - 1
      latest_week_wow_prev <- max_week_num_prev_year
    } else {
      latest_year_wow_prev <- max(df_week$year)
      latest_week_wow_prev <- latest_week_year - 1
    }
    
    # Getting MoM latest year + week and year + week of month prior
    if (latest_week_year <= 4) {
      latest_year_mom_prev <- max(df_week$year) - 1
      latest_week_mom_prev <- max_week_num_prev_year - 4 + latest_week_year
    } else {
      latest_year_mom_prev <- max(df_week$year)
      latest_week_mom_prev <- latest_week_year - 4
    }

    if (time_change == "Month-over-month") {
      df_week %>%
        filter((year == max(year) & week == latest_week_year) | (year == latest_year_mom_prev & week == latest_week_mom_prev)) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } else if (time_change == "Year-over-year") {
      df_week %>%
        filter(year %in% c(max(year), max(year) - 1) & week == latest_week_year) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } else if (time_change == "2019 to present") {
      df_week %>%
        filter(year %in% c(max(year), 2019) & week == latest_week_year) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } else if (time_change == "Week-over-week") {
      df_week %>%
        filter((year == max(year) & week == latest_week_year) | (year == latest_year_wow_prev & week == latest_week_wow_prev)) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    }

  } else {

    if (time_change == "Month-over-month") {
      df %>%
        filter(date %in% c(max(date), max(date) %m-% months(1))) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } else if (time_change == "Year-over-year") {
      df %>%
        filter(date %in% c(max(date), max(date) %m-% months(12))) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } else if (time_change == "2019 to present") {
      df %>%
        filter(date %in% c(max(date), base::as.Date(
          paste(2019, month(max(date)), day(max(date)), sep = "-")
        ))) %>%
        arrange(!!level_name, desc(date)) %>%
        mutate(pct = round(((value - lead(value)) / lead(value)) * 100, 1)) %>%
        filter(date == max(date)) -> df_pct_chg
    } 

  }
  
  return(df_pct_chg)
}

# Function to create monthly medians for data frames
create_monthly_medians <- function(df, level) {
  
  if (level == "state") {
    df %>% 
      mutate(month = rollback(date, roll_to_first = T),
             max_date = if_else(day(max(date)) >= 15, max(date), rollforward(max(date) %m-% months(1))),
             min_date = if_else(day(max_date) >= 15, floor_date(max_date %m-% months(3), "months"), floor_date(max_date %m-% months(4), "months"))
      ) %>% 
      filter(between(date, unique(min_date), unique(max_date))) %>% 
      select(-min_date) %>% 
      group_split(state) %>% 
      map(function(x) {
        y <- x %>% 
          group_by(month, state, max_date) %>% 
          summarize(pct = median(value, na.rm = T), .groups = "drop")
        
        return(y)
      }) %>% 
      map(~rename(.x, date = max_date)) -> df_list_agg
    
  } else {
    
    df %>% 
      mutate(month = rollback(date, roll_to_first = T),
             max_date = if_else(day(max(date)) >= 15, max(date), rollforward(max(date) %m-% months(1))),
             min_date = if_else(day(max_date) >= 15, floor_date(max_date %m-% months(3), "months"), floor_date(max_date %m-% months(4), "months"))
      ) %>% 
      filter(between(date, unique(min_date), unique(max_date))) %>% 
      select(-min_date) %>% 
      group_split(state, county) %>% 
      map(function(x) {
        y <- x %>% 
          group_by(month, state, county, max_date) %>% 
          summarize(pct = median(value, na.rm = T), .groups = "drop")
        
        return(y)
      }) %>% 
      map(~rename(.x, date = max_date)) -> df_list_agg
    
  }
  
  return(df_list_agg)
  
}

# Function to create mobility breakouts
make_mobility_and_spending_charts <- function(c, df, type) {
  col_name <- sym(c)
  
  if (type == "mobility") {
    
    if (c == "gps_transit_stations") {
      df <- df %>% filter(county != "Litchfield")
    }
    
    chart_title <- paste(str_to_title(
      str_replace_all(
        str_remove(
          c, "gps_"
        ), "_", " "
      )
    ), "Travel")
    
    # Trailing seven-day average for time series
    df %>% 
      select(date, county, state, countyfips, !!col_name) %>% 
      mutate(county = paste0(county, ', ', state)) %>% 
      select(-state) %>% 
      pivot_wider(!countyfips, names_from = county, values_from = !!col_name) %>% 
      drop_na() %>% 
      mutate(across(contains(", "), ~rollmean(.x, k = 7, fill = NA_real_, align = "right"))) %>% 
      drop_na() %>% 
      pivot_longer(!date, names_to = "county", values_to = "value") %>% 
      mutate(value = round(value, 2)) %>% 
      separate(col = county, into = c("county", "state"), sep = ", ") %>% 
      group_split(state) %>% 
      walk(~time_series_line_graph(df = .x, measure = chart_title, 
                                   data_freq = "daily",
                                   value_format = "percent",
                                   geo_level = "county",
                                   item = "county",
                                   caption = mobility_ts_caption))
    Sys.sleep(2)
    # Monthly medians
    df %>% 
      select(date, county, state, countyfips, !!col_name) %>% 
      mutate(county = paste0(county, ', ', state)) %>% 
      select(-state) %>% 
      pivot_wider(!countyfips, names_from = county, values_from = !!col_name) %>% 
      drop_na() %>%
      pivot_longer(!date, names_to = "county", values_to = "value") %>% 
      mutate(value = round(value, 2)) %>% 
      separate(col = county, into = c("county", "state"), sep = ", ") %>% 
      create_monthly_medians(level = "county") %>% 
      walk(~comparison_bar_graph(df = .x,
                                 agg = T,
                                 measure = chart_title,
                                 time_comparison = "latest",
                                 data_freq = "daily",
                                 geo_level = "county",
                                 item = "month",
                                 caption = mobility_agg_caption))
    
  } else if (type == "spending") {
    
    read_csv("./reference_files/affinity_codes.csv",
             col_names = T,
             col_types = "cc") -> affinity_codes
    
    pull(
      affinity_codes[which(affinity_codes$affinity_code == c), "affinity_desc"]
      ) -> chart_title
    
    # Time Series
    df %>% 
      select(date, state, !!col_name) %>%
      pivot_wider(names_from = state, values_from = !!col_name) %>% 
      drop_na() %>% 
      mutate(across(!date, ~rollmean(.x, k = 7, fill = NA_real_, align = "right"))) %>% 
      drop_na() %>% 
      pivot_longer(!date, names_to = "state", values_to = "value") %>% 
      mutate(value = round(value, 2)) %>% 
      time_series_line_graph(measure = chart_title, 
                             data_freq = "daily",
                             value_format = "percent",
                             geo_level = "state",
                             item = "state",
                             caption = consumer_spending_ts_caption)
    
    # Monthly Medians
    df %>% 
      select(date, state, !!col_name) %>% 
      rename(value = !!col_name) %>% 
      mutate(value = round(value, 2)) %>% 
      create_monthly_medians(level = "state") %>% 
      walk(~comparison_bar_graph(df = .x,
                                 agg = T,
                                 measure = chart_title,
                                 time_comparison = "latest",
                                 data_freq = "daily",
                                 geo_level = "state",
                                 item = "month",
                                 caption = consumer_spending_agg_caption))
    

  }
  
  
}

# Function to make mobility by state
make_mobility_spending_by_state <- function(c, df) {
  
  col_name <- sym(c)
  
  
  if (c == "gps_transit_stations") {
    df <- df %>% filter(county != "Litchfield")
  }
  
  if (c == "value") {
    chart_title <- "Consumer Spending"
    chart_caption <- consumer_spending_agg_caption
    
  } else {
    chart_title <- paste(str_to_title(
      str_replace_all(
        str_remove(
          c, "gps_"
        ), "_", " "
      )
    ), "Travel")
    chart_caption <- mobility_agg_caption
  }
  
  df %>% 
    select(date, county, state, !!col_name) %>% 
    mutate(month = month(date),
           year = year(date),
           max_month = if_else(day(max(date)) >= 15, 
                               month(max(date)), month(max(date) %m-% months(1)))) %>% 
    filter(year == max(year), month == max_month) %>% 
    group_by(year, month, county, state) %>%
    summarize(pct = median(!!col_name, na.rm = T), .groups = "drop") %>% 
    mutate(date = base::as.Date(paste0(year, "-", month, "-01"))) %>% 
    select(-c(year, month)) %>% 
    comparison_bar_graph(measure = chart_title,
                         time_comparison = "latest",
                         data_freq = "monthly",
                         geo_level = "county",
                         item = "county",
                         caption = chart_caption)
  
}


# Function to make trailing seven-day averages
make_trailing_seven <- function(df) {
  df %>% 
    group_split(state) %>% 
    map(function(x) {
      y <- x %>% 
        mutate(value = round(
          rollmean(value, k = 7, fill = NA, align = "right"), 1)
        ) %>% 
        filter(!is.na(value))
      return(y)
    }) %>% 
    map_df(bind_rows) -> df_trail_seven
  
  return(df_trail_seven)
}

# Base ggplot2 theme line graph
news12_theme_line <- function() {
    theme_classic() +
    theme(legend.title = element_blank(),
          legend.position = "top",
          axis.title = element_blank())
}

# Base ggplot2 theme bar graph
news12_theme_bar <- function() {
  theme_classic() +
    theme(legend.title = element_blank(),
          legend.position = "none",
          axis.title = element_blank()
          )
}

# Time series ggplot2 function
time_series_line_graph <- function(df, measure, data_freq, item,
                                   value_format, geo_level, caption) {
  
    item_name <- sym(item)
    
    
    if (!("state" %in% names(df))) {
      state_name <- "Tri-state area"
    } else if (length(unique(df$state)) != 1) {
      state_name <- "Tri-state area"
    } else {
      unique(df$state)[1] -> state_name
    }
    
    if ("industry_name" %in% names(df)) {
      industry <- unique(df$industry_name)
    } else {
      industry <- NULL
    }
    
    chart_title <- case_when(
      measure == "Hourly Wages" ~ paste(industry, measure),
      geo_level == "state" ~ measure,
      T ~ paste0(measure, " in ", state_name)
    )

    plt <- ggplot(df, mapping = aes(x = date, y = value)) +
      expand_limits(y = c(min(df$value) - 2, max(df$value) + 3)) +
      geom_line(aes(color = !!item_name), 
                size = 1.5, lineend = "round") +
      geom_label_repel(data = filter(group_by(df, state), date == max(date)),
                       aes(color = !!item_name, 
                           label = eval(parse(text = case_when(
                             value_format == "percent" ~ "label_percent(accuracy = 0.1, scale = 1)(value)", #"paste0(value, '%')"
                             value_format == "dollar" ~ "label_dollar(scale = 1)(value)", #"paste0('$', value)"
                             value_format == "number" ~ "label_number(scale = 1, big.mark = ',')(value)" #"value"
                           )))),
                       show.legend = F) +
      scale_x_date(date_breaks = "3 months", date_labels = "%b '%y") +
      scale_y_continuous(labels = eval(parse(text = case_when(
        value_format == "percent" ~ "label_percent(scale = 1)",
        value_format == "dollar" ~ "label_dollar(scale = 1)",
        value_format == "number" ~ "label_number(scale = 1, big.mark = ',')"
      )))) +
      news12_theme_line() +
      labs(title = chart_title,
           subtitle = paste0("As of ", 
                             format(max(df$date), if_else(data_freq == "daily",
                                                                 "%m/%d/%y",
                                                                 "%b '%y"))
                             ),
           caption = caption)
    
    case_when(
      measure == "Hourly Wages" ~ paste0(paste(industry, measure, state_name, sep = "_"), ".png"),
      geo_level == "state" ~ paste0(measure, ".png"),
      T ~ paste0(measure, "_", state_name, ".png")) -> filename_raw
    
    str_remove_all( 
      str_replace_all(
        str_replace_all(filename_raw,
                        "\\s+", 
                        "_"),
        "_{2,}", 
        "_"
      ),
      ","
    ) -> filename_end
    
    case_when(str_detect(caption, "Community Mobility") ~ paste0("google_community_mobility/", filename_end),
              str_detect(caption, "Affinity") ~ paste0("affinity_consumer_spending/", filename_end),
              T ~ filename_end) -> graphic_filename
    
    message(paste("Saving file", graphic_filename))
    
    ggsave(filename = paste0("./visualizations/time_series/", graphic_filename), 
           plot = plt, width = 2666, height = 1645, units = "px")
    
    
    
}

# Bar graph ggplot2 function

comparison_bar_graph <- function(df, agg = F, measure, time_comparison, 
                                 data_freq, geo_level, item, caption) {
  
  item_name <- sym(item)
  
  if (!("state" %in% names(df))) {
    state_name <- "Tri-state area"
  } else if (length(unique(df$state)) != 1) {
    state_name <- "Tri-state area"
  } else {
    unique(df$state)[1] -> state_name
  }
  
  if (geo_level == "state") {
    county_name <- NULL
  } else if (length(unique(df$county)) != 1) {
    county_name <- NULL
  } else {
    unique(df$county)[1] -> county_name
  }
  
  if ("direction" %in% names(df)) {
    direction <- unique(df$direction)
  } else {
    direction <- NULL
  }
  
  if ("display_level" %in% names(df)) {
    display_level <- unique(df$display_level)
  } else {
    display_level <- NULL
  }
  
  
  # Chart titles
  state_chg_title <- paste0("Change in ", measure, " in ", state_name)
  county_chg_title <- paste0("Change in ", measure, " in ", 
                             county_name, ", ", state_name)
  latest_title <- paste0(measure, " in ", state_name)
  
  chart_title <- case_when(
    measure == "Consumer Prices" ~ paste0("Commodities with largest price ",
                                          direction,
                                          " in ",
                                          state_name),
    measure == "Hourly Wages" ~ paste0("Industries with largest wage ",
                                       direction,
                                       " in ",
                                       state_name),
    (time_comparison == "latest" & !agg) ~ latest_title,
    (agg & time_comparison == "latest" & geo_level == "county") ~ county_chg_title,
    T ~ state_chg_title
  )
            
  
  chart_subtitle <- if_else(
    time_comparison == "latest",
    paste0("As of ", format(max(df$date), if_else(data_freq == "monthly", 
                                                  "%b '%y", 
                                                  "%m/%d/%y"))),
    paste0(time_comparison, " as of ", format(max(df$date), 
                                              if_else(data_freq == "monthly",
                                                                    "%b '%y",
                                                                    "%m/%d/%y"))))
  
  # Chart breaks
  if (min(df$pct, na.rm = T) < 0) {
    pct_breaks <- seq(from = floor(min(df$pct, na.rm = T)), 
                      to = ceiling(max(df$pct, na.rm = T)), length.out = 5)
  } else {
    pct_breaks <- seq(from = 0, 
                      to = ceiling(max(df$pct, na.rm = T)), length.out = 5)
  }

    
    if (agg) {
      plt <- ggplot(df, aes(x = !!item_name, y = pct, fill = pct)) + 
        expand_limits(y = c(min(df$pct) - 1, max(df$pct) + 1)) +
        geom_col() +
        geom_text(aes(y = pct + .33 * sign(pct), 
                      label = paste0(round(pct, 1), "%")),
                  position = position_dodge(0.9),
                  size = 5,
                  color = "black") +
        scale_fill_steps2(breaks = pct_breaks,
                          low = "#a50026", 
                          mid = "#ffffbf", 
                          high = "#91cf60",
                          midpoint = 0) + 
        scale_x_date(date_labels = "%b %Y") +
        scale_y_continuous(labels = label_percent(scale = 1)) +
        geom_hline(yintercept = 0, linetype = "dashed") +
        news12_theme_bar() +
        labs(title = chart_title,
             subtitle = chart_subtitle,
             caption = caption)
      
    } else {
      plt <- ggplot(df, aes(x = reorder(!!item_name, pct), y = pct, fill = pct)) + 
        expand_limits(y = c(min(df$pct) - 1, max(df$pct) + 1)) +
        geom_col() + 
        geom_text(aes(y = pct + .33 * sign(pct), 
                      label = paste0(round(pct, 1), "%")),
                      position = position_dodge(0.9),
                  size = 5,
                  color = "black") +
        scale_fill_steps2(breaks = pct_breaks,
                          low = "#a50026", 
                          mid = "#ffffbf", 
                          high = "#91cf60",
                          midpoint = 0) +
        scale_y_continuous(labels = label_percent(scale = 1, accuracy = 1)) +
        geom_hline(yintercept = 0, linetype = "dashed") +
        coord_flip() +
        news12_theme_bar() +
        labs(title = chart_title,
             subtitle = chart_subtitle,
             caption = caption)
    }
  
  # Chart filenames
  case_when(
    measure == "Hourly Wages" ~ paste0(
      paste("Hourly Wage",
            direction,
            "in",
            state_name, 
            time_comparison, sep = "_"), ".png"
    ),
    measure == "Consumer Prices" ~ paste0(paste("CPI",
                                                time_comparison,
                                                direction,
                                                "in",
                                                state_name,
                                                display_level,
                                                sep = "_"), ".png"),
    (!agg & geo_level == "state") ~ paste0(paste(measure, time_comparison, sep = "_"), ".png"),
    (!agg & geo_level == "county") ~ paste0(paste(measure, time_comparison, state_name, sep = "_"), ".png"),
    T ~ paste0(paste(measure, time_comparison, county_name, state_name, sep = "_"), ".png")
  ) -> filename_raw
  
   str_remove_all( 
    str_replace_all(
      str_replace_all(filename_raw,
                      "\\s+", 
                      "_"),
                    "_{2,}", 
                    "_"
      ),
      ","
    ) -> filename_end
  
  case_when(str_detect(caption, "Community Mobility") ~ paste0("google_community_mobility/", filename_end),
            str_detect(caption, "Affinity") ~ paste0("affinity_consumer_spending/", filename_end),
            T ~ filename_end) -> graphic_filename
  
  message(paste("Saving file", graphic_filename))
  
  ggsave(filename = paste0("./visualizations/state_county_bars/",
                           graphic_filename),
         plot = plt, width = 2666, height = 1645, units = "px"
         )
  
}

### Data reading sections ###
## Inflation
nyc_msa_inflation <- tryCatch({
  
  general_cpi_nyc_msa <- read_delim(file = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems",
                                    delim = "\t", trim_ws = T, col_names = T,
                                    col_types = "cccd") %>% 
    filter(series_id == "CUURS12ASA0") %>% 
    mutate(date = base::as.Date(paste0(year, 
                                       "-", 
                                       str_sub(period, 2, 3),
                                       "-01"))) %>% 
    filter(!is.na(date)) %>% 
    select(date, value)
  
  latest_nyc_msa_cpi_value <- filter(general_cpi_nyc_msa, date == max(date))$value
  
  cpi_inf <- general_cpi_nyc_msa %>% 
    mutate(inf_pct = ((latest_nyc_msa_cpi_value - value) / value) + 1) %>% 
    select(-value)
  
},
error = function(e) {
  e_full <- error_cnd(class = "inflation_error", message = paste(
    "An error occured with the inflation update:", e, "on", 
    Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}

)


## Unemployment

county_unemployment <- tryCatch({

  ## First LAUS areas
  
  laus_areas <- read_delim(file = "https://download.bls.gov/pub/time.series/la/la.area",
                           delim = "\t", trim_ws = T, col_names = T,
                           col_types = cols(.default = col_character()))
  
  Sys.sleep(2)
  
  ## Getting LAUS by News12 county from BLS
  ## https://www.bls.gov/lau/data.htm
  read_delim(file = "https://download.bls.gov/pub/time.series/la/la.data.64.County", 
             delim = "\t", trim_ws = T, col_names = T, col_types = "cccdc") %>% 
    filter(as.integer(year) >= 2019, 
           str_sub(series_id, 6, 10) %in% news_twelve_tri_state_no_nyc,
           str_sub(series_id, 3, 3) == "U") %>% 
    mutate(fips_code = str_sub(series_id, 6, 10),
           area_code = str_sub(series_id, 4, 18),
           date = base::as.Date(paste0(year, 
                                       "-", 
                                       str_sub(period, 2, 3),
                                       "-01"))) %>% 
    filter(!is.na(date)) -> laus_all_tri_state_no_label
  
  Sys.sleep(5)
  
  # Getting LAUS News12 NYC from BLS
  # https://www.bls.gov/lau/data.htm
  read_delim(file = "https://download.bls.gov/pub/time.series/la/la.data.65.City", 
             delim = "\t", trim_ws = T, col_names = T, col_types = "cccdc") %>% 
    filter(as.integer(year) >= 2019, 
           str_sub(series_id, 4, 18) == "CT3651000000000",
           str_sub(series_id, 3, 3) == "U") %>% 
    mutate(fips_code = str_sub(series_id, 6, 10),
           area_code = str_sub(series_id, 4, 18),
           date = base::as.Date(paste0(year, 
                                       "-", 
                                       str_sub(period, 2, 3),
                                       "-01"))) %>% 
    filter(!is.na(date)) -> laus_nyc_no_label
  
  Sys.sleep(5)
  
  ## Unemployment Rate ##
  cleaning_laus(df_county = laus_all_tri_state_no_label,
                df_city = laus_nyc_no_label,
                measure = "03",
                area_df = laus_areas) -> laus_unemprate_tri_state_list
  
  laus_unemprate_tri_state_full <- laus_unemprate_tri_state_list %>% nth(1)
  max_combo_unemprate_date <- laus_unemprate_tri_state_list %>% nth(2)
  
  write_csv(laus_unemprate_tri_state_full,
            "./data/unemployment_rate_long.csv")
  
  laus_unemprate_tri_state_full %>% 
    unite(col = "county", c(county, state), sep = ", ") %>% 
    select(-c(series_id, fips_code)) %>% 
    pivot_wider(names_from = county, values_from = pct) %>% 
    write_csv("./data/unemployment_rate_wide.csv")
    
  
  ## Employment Levels ##
  cleaning_laus(df_county = laus_all_tri_state_no_label,
                df_city = laus_nyc_no_label,
                measure = "05",
                area_df = laus_areas) -> laus_emp_tri_state_list
  
  laus_emp_tri_state_full <- laus_emp_tri_state_list %>% nth(1)
  max_combo_emp_date <- laus_emp_tri_state_list %>% nth(2)
  
  
  write_csv(laus_emp_tri_state_full,
            "./data/employment_levels_long.csv")
  
  laus_emp_tri_state_full %>% 
    unite(col = "county", c(county, state), sep = ", ") %>% 
    select(-c(series_id, fips_code)) %>% 
    pivot_wider(names_from = county, values_from = value) %>% 
    write_csv("./data/employment_levels_wide.csv")
  
  ## Visualizations:
  # Line graph time series
  laus_unemprate_tri_state_full %>%
    filter(date <= max_combo_unemprate_date) %>% 
    rename(value = pct) %>% 
    group_split(state) %>% 
    walk(~time_series_line_graph(.x, measure = "Unemployment Rate",
                                 data_freq = "monthly",
                                 value_format = "percent",
                                 geo_level = "county",
                                 item = "county",
                                 caption = bls_caption))
  
  # Bar graph latest
  laus_unemprate_tri_state_full %>% 
    filter(date == max_combo_unemprate_date) %>% 
    group_split(state) %>% 
    walk(~comparison_bar_graph(.x, measure = "Unemployment Rate",
                               time_comparison = "latest",
                               data_freq = "monthly",
                               geo_level = "county",
                               item = "county",
                               caption = bls_caption))
  
  laus_emp_tri_state_full %>% 
    filter(date <= max_combo_emp_date) -> laus_emp_tri_state_aligned
  
  # Line graph employment 
  laus_emp_tri_state_aligned %>%
    filter(date <= max_combo_emp_date) %>% 
    group_split(state) %>% 
    walk(~time_series_line_graph(.x, measure = "Employment Levels",
                                 data_freq = "monthly",
                                 value_format = "number",
                                 geo_level = "county",
                                 item = "county",
                                 caption = bls_caption))
  
  
  ## Bar graph employment MoM, YoY, 2019 - present
  
  
  walk(time_frames, function(x) {
    laus_emp_tri_state_aligned %>% 
      change_time_bar_calc(time_change = x, level = "county") %>% 
      group_split(state) %>% 
      walk(~comparison_bar_graph(df = .x,
                                 measure = "Employment Levels",
                                 time_comparison = x,
                                 data_freq = "monthly",
                                 geo_level = "county",
                                 item = "county",
                                 caption = bls_caption))
    
  })
  
},
error = function(e) {
  e_full <- error_cnd(class = "county_unemployment_error", message = paste(
    "An error occured with the county unemployment update:", e, "on", 
    Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
})

Sys.sleep(3)


restaurant_reservations_jobs <- tryCatch({
  
  # Getting restaurant data from OpenTable for CT, NJ, and NY
  # https://www.opentable.com/state-of-industry
  ot_scripts <- read_html("https://www.opentable.com/state-of-industry") %>%
    html_nodes("script")
  
  ot_data_script <- which(
    map_lgl(
      map_chr(ot_scripts, html_text), ~str_detect(.x, "__INITIAL_STATE__")
      )
    )
  
  ot_raw_text <- ot_scripts %>%
    nth(ot_data_script) %>%
    html_text()
  
  Sys.sleep(5)
  
  ot_raw_json <- ot_raw_text %>%
    str_match(regex("w.__INITIAL_STATE__ =\\s+(.*)", dotall = T)) %>%
    nth(2) %>%
    str_remove(";\\}\\)\\(window\\);$")
  
  
  Sys.sleep(5)
  
  ot_data <- ot_raw_json %>%
    fromJSON(json_str = .)
  
  
  ot_chr_dates <- ot_data %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("headers")
  
  ot_dates <- ymd(ot_chr_dates) %>% sort()
  
  ot_res_tri_state <- ot_data %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("states") %>%
    map_dfr(., ~tibble(
      value = .$yoy,
      state = .$name
    )) %>%
    filter(state %in% c("Connecticut", "New Jersey", "New York")) %>% 
    mutate(date = rep(ot_dates, 3))
  
  ot_res_nyc <- ot_data %>%
    extract2("covidDataCenter") %>%
    extract2("fullbook") %>%
    extract2("cities") %>%
    map_dfr(., ~tibble(
      value = .$yoy,
      state = .$name
    )) %>%
    filter(state == "New York") %>% 
    mutate(date = ot_dates,
           state = "NYC")
  
  ot_res_tri_state_full <- bind_rows(ot_res_tri_state, ot_res_nyc)
  
  Sys.sleep(5)

  # Getting Restaurant Employment Numbers by State from BLS
  # https://www.bls.gov/sae/data/
  read_delim(file = "https://download.bls.gov/pub/time.series/sm/sm.data.1.AllData",
             delim = "\t", trim_ws = T, col_names = T,
             col_types = "cccdc") %>% 
    filter(series_id %in% c("SMU09000007072250001",
                            "SMU34000007072250001",
                            "SMU36000007072250001"),
           as.integer(year) >= 2019,
           period != "M13") %>% 
    mutate(state_code = str_sub(series_id, 4, 5),
           date = base::as.Date(paste0(year, 
                                       "-", 
                                       str_sub(period, 2, 3),
                                       "-01"))) -> res_job_tri_state_no_label
  
  read_delim(file = "https://download.bls.gov/pub/time.series/sm/sm.state",
             delim = "\t", trim_ws = T, col_names = T,
             col_types = "cc") -> state_fips_bls
  
  
  res_job_tri_state <- res_job_tri_state_no_label %>% 
    inner_join(state_fips_bls, by = "state_code") %>% 
    select(series_id, date, value, state_code, state_name) %>% 
    rename(state = state_name) %>% 
    mutate(value = value * 1000)
  
  # Getting 7 day average of OpenTable data
  ot_res_tri_state_full %>% 
    make_trailing_seven() -> ot_tri_state_trail_seven
  
  write_csv(ot_res_tri_state_full, 
            "./data/restaurant_reservations_long.csv")
  
  ot_res_tri_state_full %>% 
    pivot_wider(names_from = state, values_from = value) %>% 
    write_csv('./data/restaurant_reservations_wide.csv')
  
  write_csv(res_job_tri_state, 
            "./data/restaurant_jobs_long.csv")
  
  res_job_tri_state %>% 
    select(-c(series_id, state_code)) %>% 
    pivot_wider(names_from = state, values_from = value) %>% 
    write_csv("./data/restaurant_jobs_wide.csv")
  
  ## Visualizations
  # Line graph of seven-day trailing average by state
  time_series_line_graph(df = ot_tri_state_trail_seven,
                         measure = "Restaurant Reservations",
                         data_freq = "daily",
                         value_format = "percent",
                         geo_level = "state",
                         item = "state",
                         caption = ot_ts_caption)
  
  # Line graph of restaurant jobs by state
  time_series_line_graph(df = res_job_tri_state,
                         measure = "Restaurant Jobs",
                         data_freq = "monthly",
                         value_format = "number",
                         geo_level = "state",
                         item = "state",
                         caption = bls_caption)

  
  # Bar graph of each state with median change for latest 4 months
  
  ot_res_tri_state_full %>%
    create_monthly_medians(level = "state") %>% 
    walk(~comparison_bar_graph(.x,
                               agg = T,
                               measure = "Restaurant Reservations",
                               time_comparison = "latest",
                               data_freq = "daily",
                               geo_level = "state",
                               item = "month",
                               caption = ot_agg_caption))
  
  
  ## Bar graph of MoM, YoY, & 2019 - Present percent change in restaurant jobs
  walk(time_frames, function(x) {
    res_job_tri_state %>% 
      change_time_bar_calc(time_change = x, level = "state") %>% 
      comparison_bar_graph(measure = "Restaurant Jobs",
                           time_comparison = x,
                           data_freq = "monthly",
                           geo_level = "state",
                           item = "state",
                           caption = bls_caption)
  })
  
  
},
error = function(e) {
  e_full <- error_cnd(class = "restaurant_error", message = paste(
    "An error occured with the restaurant update:", e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

## Mobility

mobility_transit <- tryCatch({
  
  # Getting Google's COVID-19 community transit data via Opportunity Insights
  # GitHub repo: https://github.com/OpportunityInsights/EconomicTracker
  # Data dictionary (what do columns mean): 
  # https://github.com/OpportunityInsights/EconomicTracker/blob/main/docs/oi_tracker_data_dictionary.md
  
  # Data documentation (what does it measure):
  # https://github.com/OpportunityInsights/EconomicTracker/blob/main/docs/oi_tracker_data_documentation.md
  
  # City IDs: https://github.com/OpportunityInsights/EconomicTracker/blob/main/data/GeoIDs%20-%20City.csv
  # County IDs: https://github.com/OpportunityInsights/EconomicTracker/blob/main/data/GeoIDs%20-%20County.csv
  
  oi_google_mobility_county <- read_csv(
    file = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Google%20Mobility%20-%20County%20-%20Daily.csv",
    col_names = T, col_types = cols(.default = col_character())) %>% 
    mutate(countyfips = if_else(nchar(countyfips) < 5, paste0("0", countyfips), as.character(countyfips)),
           date = base::as.Date(paste0(year, "-", month, "-", day)),
           across(starts_with("gps_"), ~round((as.double(.x) * 100), 1))) %>% 
    filter(countyfips %in% news_twelve_tri_state_no_nyc) %>% 
    inner_join(fips_codes, by = c("countyfips" = "fips")) %>% 
    select(-c(year, month, day))
  
  Sys.sleep(5)
  
  oi_google_mobility_city <- read_csv(
    file = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Google%20Mobility%20-%20City%20-%20Daily.csv",
    col_names = T, col_types = cols(.default = col_character())) %>% 
    filter(cityid == "2") %>% 
    mutate(date = base::as.Date(paste0(year, "-", month, "-", day)),
           across(starts_with("gps_"), ~round((as.double(.x) * 100), 1)),
           county = "NYC",
           state = "NY",
           countyfips = "3651000") %>% 
    select(-c(year, month, day, cityid))
  
  Sys.sleep(3)
  
  oi_google_mobility_tri_state <- bind_rows(oi_google_mobility_county,
                                            oi_google_mobility_city)
  
  oi_google_mobility_tri_state %>% 
    pivot_longer(cols = starts_with("gps"), names_to = "measure", values_to = "pct") %>% 
    write_csv("./data/google_mobility_long.csv")
  
  oi_google_mobility_tri_state %>% 
    relocate(date, countyfips, county, state, everything()) %>% 
    write_csv("./data/google_mobility_wide.csv")
  
  names(select(oi_google_mobility_tri_state, starts_with("gps_"))) -> mobility_col_names
  
  # Getting MTA Subway, LIRR, Metro-North, and Bridges & Tunnels data
  
  mta_rows <- as.integer(Sys.Date()) - as.integer(base::as.Date("2020-03-01"))
  
  mta_res <- GET(url = paste0("https://data.ny.gov/resource/vxuj-8kew.csv?$limit=", 
                              mta_rows),
                 authenticate(user = "anesta@dotdash.com", password = SCT_PW))
  
  stop_for_status(mta_res)
  
  mta_full_raw <- content(mta_res, encoding = "UTF-8", 
                          col_types = cols(.default = col_character()))
  
  mta_full_raw %>% 
  mutate(date = base::as.Date(date),
         across(!date, as.numeric),
         across(contains("of"), function(x) x * 100)) -> mta_data
  
  mta_data %>% 
    arrange(date) %>% 
    write_csv("./data/mta_usage_wide.csv")
  
  mta_data_long_clean <- mta_data %>% 
    select(date, contains("of")) %>% 
    pivot_longer(!date, names_to = "state", values_to = "value") %>% 
    mutate(state = str_to_title(
      str_trim(
        str_remove(str_replace_all(state, "_", " "), "of.*")
        )
      )
    ) %>% 
    mutate(state = if_else(state == "Lirr", str_to_upper(state), state))
  
  
  mta_data_long_clean %>% 
    write_csv("./data/mta_usage_long.csv")
  
  ## Visualizations:
  # By state counties trailing seven day average over time line graph
  # By state bar graph of each county latest 4 months median
  
  walk(mobility_col_names, ~make_mobility_and_spending_charts(c = .x, 
                                                              df = oi_google_mobility_tri_state,
                                                              type = "mobility"))
  
  oi_google_mobility_tri_state %>% 
    group_split(state) %>% 
    walk(function(x) {
      walk(mobility_col_names, function(y) {
        make_mobility_spending_by_state(c = y, df = x)
      })
    })
  
  # By transit type trailing seven day average over time line graph
  mta_data_long_clean %>% 
    arrange(date) %>% 
    filter(!is.na(value)) %>% 
    make_trailing_seven() -> transit_method_trailing_seven
  
  time_series_line_graph(df = transit_method_trailing_seven,
                         measure = "Transit Methods",
                         data_freq = "daily",
                         value_format = "percent",
                         geo_level = "state",
                         item = "state",
                         caption = mta_caption)
  

  
  # By transit bar graph latest 4 months median
  mta_data_long_clean %>% 
    filter(!is.na(value)) %>%
    mutate(value = value - 100) %>% 
    create_monthly_medians(level = "state") %>% 
    walk(~comparison_bar_graph(.x,
                               agg = T,
                               measure = "Transit Usage",
                               time_comparison = "latest",
                               data_freq = "daily",
                               geo_level = "state",
                               item = "month",
                               caption = mta_caption))
  
  # Transit usage raw growth by time period
  ## Bar graph of MoM, YoY change in transit usage
  mta_data %>%  
    filter(date <= if_else(day(max(date)) >= 15, max(date), rollforward(max(date) %m-% months(1)))) %>% 
    select(date, contains("total")) %>% 
    pivot_longer(!date, names_to = "state", values_to = "value") %>% 
    mutate(state = str_to_title(
      str_trim(
        str_remove(str_replace_all(state, "_", " "), "total.*")
      )
    )
    ) %>% 
    filter(!is.na(value)) %>% 
    mutate(state = if_else(state == "Lirr", str_to_upper(state), state),
           month = rollback(date, roll_to_first = T)) %>% 
    group_by(state, month) %>% 
    summarize(value = median(value, na.rm = T), .groups = "drop") %>%  
    rename(date = month) -> transit_tri_state_monthly_med
  
  # Monthly total
  mta_data %>%  
    filter(date <= if_else(day(max(date)) >= 15, max(date), rollforward(max(date) %m-% months(1)))) %>% 
    select(date, contains("total")) %>% 
    pivot_longer(!date, names_to = "state", values_to = "value") %>% 
    mutate(state = str_to_title(
      str_trim(
        str_remove(str_replace_all(state, "_", " "), "total.*")
      )
    )
    ) %>% 
    filter(!is.na(value)) %>% 
    mutate(state = if_else(state == "Lirr", str_to_upper(state), state),
           month = rollback(date, roll_to_first = T)) %>% 
    group_by(state, month) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(date = month) -> transit_tri_state_monthly_sum
  
  # Read in old monthly mta data
  read_csv("./reference_files/old_mta_full_monthly.csv",
           col_names = T,
           col_types = "cDd") -> transit_tri_state_monthly_sum_old
  
  
  transit_tri_state_monthly_sum_full <- bind_rows(
    transit_tri_state_monthly_sum, transit_tri_state_monthly_sum_old
  )
  
  walk(list(time_frames[[1]], time_frames[[2]]), function(x) {
    transit_tri_state_monthly_med %>% 
      change_time_bar_calc(time_change = x, level = "state") %>% 
      comparison_bar_graph(measure = "Transit Usage",
                           time_comparison = x,
                           data_freq = "monthly",
                           geo_level = "state",
                           item = "state",
                           caption = mta_caption)
  })
  
  transit_tri_state_monthly_sum_full %>% 
    change_time_bar_calc(time_change = "2019 to present", level = "state") %>% 
    comparison_bar_graph(measure = "Transit Usage",
                         time_comparison = "Pre-pandemic average to present",
                         data_freq = "monthly",
                         geo_level = "state",
                         item = "state",
                         caption = mta_monthly_prepan)
  
  
  # Most recent end of week
  mta_data %>%
    mutate(week_day = wday(date)) %>% 
    group_by(week_day) %>%
    summarize(m_wday = max(date), .groups = "drop") %>% 
    filter(week_day == 6) %>% 
    pull(m_wday) -> mta_m_wday
  
  ## WoW
  transit_tri_state_weekly_med <- mta_data %>% 
    filter(date <= mta_m_wday) %>% 
    select(date, contains("total")) %>% 
    pivot_longer(!date, names_to = "state", values_to = "value") %>% 
    mutate(state = str_to_title(
      str_trim(
        str_remove(str_replace_all(state, "_", " "), "total.*")
      )
    )
    ) %>% 
    filter(!is.na(value)) %>% 
    mutate(state = if_else(state == "Lirr", str_to_upper(state), state),
           year = year(date),
           week = week(date)) %>% 
    group_by(state, year, week) %>% 
    summarize(value = median(value, na.rm = T), .groups = "drop") %>% 
    mutate(date = base::as.Date(paste0(year, str_pad(week, 2, "left", "0"), "6"), "%Y%U%u")) %>% 
    filter(week != 53) %>% 
    mutate(date = if_else(is.na(date), lag(date, 1) + days(7), date))
  
  transit_tri_state_weekly_med %>% 
    change_time_bar_calc(time_change = "Week-over-week", "state", weekly = T) %>% 
    comparison_bar_graph(measure = "Transit Usage", 
                         time_comparison = "Week-over-week",
                         data_freq = "weekly",
                         geo_level = "state",
                         item = "state",
                         caption = mta_caption)
  
},
error = function(e) {
  e_full <- error_cnd(class = "mobility_transit_error", message = paste(
    "An error occured with the mobility and transit update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

## Consumer Spending

consumer_spending <- tryCatch({
  
  # Getting consumer spending data from Affinity (change from Jan '20 levels)
  # Github Repo: https://github.com/OpportunityInsights/EconomicTracker
  # Data dictionary (what do columns mean): 
  # https://github.com/OpportunityInsights/EconomicTracker/blob/main/docs/oi_tracker_data_dictionary.md
  
  # Data documentation (what does it measure):
  # https://github.com/OpportunityInsights/EconomicTracker/blob/main/docs/oi_tracker_data_documentation.md
  
  # City IDs: https://github.com/OpportunityInsights/EconomicTracker/blob/main/data/GeoIDs%20-%20City.csv
  # County IDs: https://github.com/OpportunityInsights/EconomicTracker/blob/main/data/GeoIDs%20-%20County.csv
  
  ## City data
  read_csv(file = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Affinity%20-%20City%20-%20Daily.csv",
           col_names = T,
           col_types = cols(.default = col_character())) %>% 
    filter(cityid == "2", provisional != "1") %>% 
    mutate(date = base::as.Date(paste(year, month, day, sep = "-")),
           value = as.double(spend_all),
           county = "NYC",
           state = "NY",
           countyfips = "3651000") %>% 
    select(date, countyfips, value, county, state) -> affinity_nyc
  
  Sys.sleep(5)
  
  ## County data
  read_csv(file = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Affinity%20-%20County%20-%20Daily.csv",
           col_names = T,
           col_types = cols(.default = col_character())) %>% 
    mutate(countyfips = if_else(nchar(countyfips) == 4L, 
                                paste0("0", countyfips), 
                                countyfips),
           value = as.double(spend_all),
           date = base::as.Date(paste(year, month, day, sep = "-"))) %>% 
    filter(countyfips %in% news_twelve_tri_state_no_nyc, provisional != "1") %>% 
    select(date, countyfips, value) %>% 
    inner_join(fips_codes, by = c("countyfips" = "fips")) -> affinity_tri_state
  
  ## State data
  read_csv(file = "https://raw.githubusercontent.com/OpportunityInsights/EconomicTracker/main/data/Affinity%20-%20State%20-%20Daily.csv",
           col_names = T,
           col_types = cols(.default = col_character())) %>% 
    mutate(across(contains("spend"), as.double),
           statefips = str_pad(statefips, 2, side = "left", pad = "0"),
           date = base::as.Date(paste(year, month, day, sep = "-"))) %>% 
    filter(date > base::as.Date("2020-01-12"), 
           provisional != "1",
           statefips %in% c("09", "34", "36")) %>% 
    mutate(state = case_when(statefips == "09" ~ "Connecticut",
                             statefips == "34" ~ "New Jersey",
                             statefips == "36" ~ "New York"),
           across(contains("spend_"), function(x) x * 100)) %>% 
    select(-c(year, month, day, freq, provisional)) -> affinity_tri_state_state_categories
  
  names(select(affinity_tri_state_state_categories, starts_with("spend_"))) -> spending_col_names
  
  # Combining city & county
  affinity_tri_state_full <- bind_rows(affinity_tri_state, affinity_nyc) %>% 
    filter(date > base::as.Date("2020-01-12")) %>% 
    mutate(value = 100 * value)
  
  write_csv(affinity_tri_state_full,
            "./data/consumer_spending_all_long.csv")
  
  affinity_tri_state_full %>% 
    select(-countyfips) %>% 
    unite(col = "county", c(county, state), sep = ", ") %>% 
    pivot_wider(names_from = county, values_from = value) %>% 
    write_csv("./data/consumer_spending_all_wide.csv")
  
  affinity_tri_state_full %>% 
    make_trailing_seven() -> affinity_tri_state_trail_seven
  
  affinity_tri_state_state_categories %>% 
    select(-statefips) %>% 
    relocate(date, state, everything()) %>% 
    write_csv("./data/consumer_spending_categories_wide.csv")
  
  affinity_tri_state_state_categories %>% 
    pivot_longer(cols = starts_with("spend"), names_to = "category", values_to = "pct") %>% 
    write_csv("./data/consumer_spending_categories_long.csv")
  
  ## Visualizations:
  # By state counties trailing seven day average over time
  affinity_tri_state_trail_seven %>% 
    group_split(state) %>% 
    walk(~time_series_line_graph(df = .x,
                                 measure = "Consumer Spending",
                                 data_freq = "daily",
                                 value_format = "percent",
                                 geo_level = "county",
                                 item = "county",
                                 caption = consumer_spending_ts_caption))

  
  # By county bar graph of each county latest 4 months median
  
  affinity_tri_state_full %>% 
    create_monthly_medians(level = "county") %>% 
    walk(~comparison_bar_graph(.x,
                               agg = T,
                               measure = "Consumer Spending",
                               time_comparison = "latest",
                               data_freq = "daily",
                               geo_level = "county",
                               item = "month",
                               caption = consumer_spending_agg_caption
                               ))
  
  # By state bar graph of latest 4 months median by spending category
  
  walk(spending_col_names, ~make_mobility_and_spending_charts(c = .x, 
                                                              df = affinity_tri_state_state_categories,
                                                              type = "spending"))
  
  # By state bar graph of latest monthly median
  affinity_tri_state_full %>% 
    group_split(state) %>% 
    walk(~make_mobility_spending_by_state(c = "value", df = .x))
  
  
  
},
error = function(e) {
  e_full <- error_cnd(class = "consumer_spending_error", message = paste(
    "An error occured with the consumer spending update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

## Home Prices

home_prices <- tryCatch({
  # From Zillow: https://www.zillow.com/research/data/
  # API Docs Here: https://documenter.getpostman.com/view/9197254/UVsFz93V
  # Citation terms here: https://bridgedataoutput.com/zillowterms
  
  # ZHVI by city
  read_csv(
    paste0(
      "https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=",
      as.integer(Sys.time())
    ),
    col_names = T,
    col_types = cols(.default = col_character())
  ) %>% 
    filter(RegionID == "6181") %>% 
    mutate(fips_code = "3651000",
           county = "NYC") %>% 
    rename(state = StateName) %>% 
    select(county, state, fips_code, 
           matches("\\d{4}-\\d{2}-\\d{2}")) %>% 
    pivot_longer(!c(county, state, fips_code), 
                 names_to = "date",
                 values_to = "value") %>% 
    mutate(date = base::as.Date(date),
           value = as.integer(value)) -> home_price_nyc
  
  Sys.sleep(2)
  
  # ZHVI by county
  read_csv(
    paste0(
      "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=",
         as.integer(Sys.time())
      ),
         col_names = T,
         col_types = cols(.default = col_character())
    ) %>% 
    mutate(fips_code = paste0(StateCodeFIPS, MunicipalCodeFIPS),
           RegionName = str_remove(RegionName, "\\s+County")) %>% 
    filter(fips_code %in% news_twelve_tri_state_no_nyc) %>% 
    select(RegionName, State, fips_code,
           matches("\\d{4}-\\d{2}-\\d{2}")) %>% 
    rename(county = RegionName,
           state = State) %>% 
    pivot_longer(!c(county, state, fips_code), 
                 names_to = "date",
                 values_to = "value") %>% 
    mutate(date = base::as.Date(date),
           value = as.integer(value)) -> home_price_tri_state
  
  home_price_tri_state_full <- bind_rows(home_price_tri_state,
                                         home_price_nyc) %>%
    mutate(date = rollback(date, roll_to_first = T)) %>% 
    filter(date >= base::as.Date("2019-01-01")) %>% 
    left_join(cpi_inf, by = "date") %>% 
    mutate(inf_pct = if_else(is.na(inf_pct), 1, inf_pct),
           value = value * inf_pct) %>% 
    select(-inf_pct)
  
  write_csv(home_price_tri_state_full,
            "./data/home_prices_long.csv")
  
  home_price_tri_state_full %>% 
    select(-fips_code) %>% 
    unite(col = "county", c(county, state), sep = ", ") %>% 
    pivot_wider(names_from = county, values_from = value) %>% 
    write_csv("./data/home_prices_wide.csv")
  
  ## Visualizations
  # By state line graph prices each county over time
  home_price_tri_state_full %>% 
    group_split(state) %>% 
    walk(~time_series_line_graph(df = .x,
                                 measure = "Home Prices",
                                 data_freq = "monthly",
                                 value_format = "dollar",
                                 geo_level = "county",
                                 item = "county",
                                 caption = paste(home_prices_caption, 
                                                 inf_caption,
                                                 sep = "\n")))
    
  
  # By state bar graph MoM, YoY, and 2019-present percent changes
  
  walk(time_frames, function(x) {
    home_price_tri_state_full %>% 
      change_time_bar_calc(time_change = x, level = "county") %>% 
      group_split(state) %>% 
      walk(~comparison_bar_graph(df = .x,
                                 measure = "Home Prices",
                                 time_comparison = x,
                                 data_freq = "monthly",
                                 geo_level = "county",
                                 item = "county",
                                 caption = paste(home_prices_caption, 
                                                 inf_caption,
                                                 sep = "\n")))
    
  })
  
  
},
error = function(e) {
  e_full <- error_cnd(class = "home_prices_error", message = paste(
    "An error occured with the home prices update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

rent_cost <- tryCatch({
  
  # From ApartmentsList
  # https://www.apartmentlist.com/research/category/data-rent-estimates
  read_html("https://www.apartmentlist.com/research/category/data-rent-estimates") %>% 
    html_nodes("script") -> rent_scripts
  
  rent_data_script <- which(
    map_lgl(
      map_chr(rent_scripts, html_text), ~str_detect(.x, "Apartment_List_Rent_Estimates_County")
    )
  )
  
  rent_raw_text <- rent_scripts %>%
    nth(rent_data_script) %>%
    html_text()
  
  Sys.sleep(5)
  
  fromJSON(json_str = rent_raw_text) -> rent_json
  
  rent_json %>% 
    extract2("props") %>% 
    extract2("pageProps") %>% 
    extract2("component") %>% 
    extract2("searchResults") %>% 
    nth(1) %>% 
    extract2("fields") %>% 
    extract2("downloadableAssets") %>% 
    nth(5) %>% 
    extract2("fields") %>% 
    extract2("attachment") %>% 
    extract2("fields") %>%
    extract2("file") %>% 
    extract2("url") -> rent_url_county
  
  rent_json %>% 
    extract2("props") %>% 
    extract2("pageProps") %>% 
    extract2("component") %>% 
    extract2("searchResults") %>% 
    nth(1) %>% 
    extract2("fields") %>% 
    extract2("downloadableAssets") %>% 
    nth(6) %>% 
    extract2("fields") %>% 
    extract2("attachment") %>% 
    extract2("fields") %>%
    extract2("file") %>% 
    extract2("url") -> rent_url_city
  
  Sys.sleep(3)
  
  # Rent by City
  read_csv(
    paste0("https:", 
           rent_url_city),
    col_names = T,
    col_types = cols(.default = col_character())
  ) %>% 
    filter(FIPS_Code == "3651000", Bedroom_Size == "_Overall") %>% 
    rename(fips_code = FIPS_Code) %>% 
    separate(col = City_Name, into = c("county", "state"), sep = ", ") %>%
    select(county, state, fips_code,
           matches("\\d{4}_\\d{2}")) %>% 
    pivot_longer(!c(county, state, fips_code), 
                 names_to = "date",
                 values_to = "value") %>% 
    mutate(county = str_remove(county, "\\s+County"),
           value = as.double(value),
           date = ymd(paste0(date, "_01"))) -> rent_nyc
  
  Sys.sleep(2)
  
  # Rent by County
  read_csv(
    paste0("https:", 
           rent_url_county),
    col_names = T,
    col_types = cols(.default = col_character())
  ) %>% 
    mutate(
      fips_code = if_else(nchar(FIPS_Code) < 5, 
                          paste0("0", FIPS_Code), 
                          as.character(FIPS_Code))
    ) %>% 
    filter(fips_code %in% news_twelve_tri_state_no_nyc,
           Bedroom_Size == "_Overall") %>% 
    separate(col = County_Name, into = c("county", "state"), sep = ", ") %>% 
    select(county, state, fips_code,
           matches("\\d{4}_\\d{2}")) %>% 
    pivot_longer(!c(county, state, fips_code), 
                 names_to = "date",
                 values_to = "value") %>% 
    mutate(county = str_remove(county, "\\s+County"),
           value = as.double(value),
           date = ymd(paste0(date, "_01"))) -> rent_tri_state 
  
  rent_tri_state_full <- bind_rows(rent_tri_state, rent_nyc) %>% 
    filter(date >= base::as.Date("2019-01-01")) %>% 
    left_join(cpi_inf, by = "date") %>% 
    mutate(inf_pct = if_else(is.na(inf_pct), 1, inf_pct),
           value = value * inf_pct) %>% 
    select(-inf_pct)
  
  write_csv(rent_tri_state_full,
            "./data/rent_cost_long.csv")
  
  rent_tri_state_full %>% 
    select(-fips_code) %>% 
    unite(col = "county", c(county, state), sep = ", ") %>% 
    pivot_wider(names_from = county, values_from = value) %>% 
    write_csv("./data/rent_cost_wide.csv")
  
  ## Visualizations
  # By state line graph prices each county over time
  rent_tri_state_full %>% 
    group_split(state) %>% 
    walk(~time_series_line_graph(df = .x,
                                 measure = "Rent Cost",
                                 data_freq = "monthly",
                                 value_format = "dollar",
                                 geo_level = "county",
                                 item = "county",
                                 caption = paste(rent_cost_caption, 
                                                 inf_caption,
                                                 sep = "\n")))
  
  # By state bar graph MoM, YoY, and 2019-present percent changes

  walk(time_frames, function(x) {
    rent_tri_state_full %>% 
      change_time_bar_calc(time_change = x, level = "county") %>% 
      group_split(state) %>% 
      walk(~comparison_bar_graph(df = .x,
                                 measure = "Rent Cost",
                                 time_comparison = x,
                                 data_freq = "monthly",
                                 geo_level = "county",
                                 item = "county",
                                 caption = paste(rent_cost_caption, 
                                                 inf_caption,
                                                 sep = "\n")))
    
  })
  
  
    
},
error = function(e) {
  e_full <- error_cnd(class = "rent_cost_error", message = paste(
    "An error occured with the rent cost update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

consumer_prices <- tryCatch({
  
  # Getting Consumer Price Changes from BLS
  # https://www.bls.gov/cpi/data.htm
  
  read_delim("https://download.bls.gov/pub/time.series/cu/cu.data.7.OtherNorthEast", delim = "\t", trim_ws = T,
             col_names = T, col_types = "cccdc") %>% 
    mutate(date = base::as.Date(paste0(year, "-", str_remove(period, "M"), "-01")))-> cpi_north_east
  
  Sys.sleep(2)
  
  read_delim("https://download.bls.gov/pub/time.series/cu/cu.item", 
             col_names = T, col_types = "cccci",
             delim = "\t", trim_ws = T) -> cpi_items
  
  cpi_north_east %>% 
    mutate(region = str_sub(series_id, start = 5, end = 8),
           period = month(date),
           year = as.integer(year),
           seas_adj = str_sub(series_id, start = 3, end = 3),
           item_code = str_sub(series_id, 9, 16),
           period_code = str_sub(series_id, 4, 4)) %>% 
    select(-footnote_codes) %>% 
    filter(date >= base::as.Date("2019-01-01"),
           region %in% c("0110", "S12A")) -> cpi_nycmsa_ne
  
  cpi_nycmsa_ne %>% 
    left_join(cpi_items, by = "item_code") %>% 
    mutate(state = if_else(region == "0110", "New England", "New York City MSA"),
           display_level = if_else(display_level %in% c("0", "1"), "broad", "detailed")) %>% 
    filter(period_code == "R", seas_adj == "U") %>% 
    select(date, state, display_level, item_name, value) -> cpi_tri_state_full
  
  cpi_tri_state_full %>% 
    arrange(item_name, state, desc(date)) %>% 
    filter(date %in% c(max(date), max(date) %m-% months(1))) %>% 
    mutate(mom_pct = round(((value - lead(value)) / lead(value)) * 100, 2)) %>% 
    filter(date == max(date)) %>% 
    write_csv("./data/cpi_mom_change.csv")
  
  cpi_tri_state_full %>% 
    arrange(item_name, state, desc(date)) %>% 
    filter(date %in% c(max(date), max(date) %m-% months(12))) %>% 
    mutate(yoy_pct = round(((value - lead(value)) / lead(value)) * 100, 2)) %>% 
    filter(date == max(date)) %>% 
    write_csv("./data/cpi_yoy_change.csv")
  
  cpi_tri_state_full %>% 
    arrange(item_name, state, desc(date)) %>% 
    filter(date %in% c(max(date), base::as.Date(paste("2019", month(max(date)), "01", sep = "-")))) %>% 
    mutate(nineteen_to_present_pct = round(((value - lead(value)) / lead(value)) * 100, 2)) %>% 
    filter(date == max(date)) %>% 
    write_csv("./data/cpi_19_present_change.csv")
  
  
  ## Visualizations
  # By state bar graph MoM, YoY, and 2019-present percent changes (top four)
  
  
  walk(time_frames, function(x) {
    
    cpi_tri_state_full %>% 
      group_split(state) %>% 
      map_df(~change_time_bar_calc(df = .x, time_change = x, level = "item_name")) %>% 
      group_split(state, display_level) %>% 
      walk(function(y) {
          
           
           risers <- slice_max(y, order_by = pct, n = 4) %>% 
             mutate(direction = "increases")
           fallers <- slice_min(y, order_by = pct, n = 4) %>% 
             mutate(direction = "decreases")
           
           cpi_caption <- if_else(unique(y$state) == "New England",
                                  bls_cpi_ne,
                                  bls_cpi_nycmsa)

           comparison_bar_graph(risers, measure = "Consumer Prices", item = "item_name",
                                time_comparison = x, data_freq = "monthly",
                                geo_level = "state", caption = cpi_caption)

           comparison_bar_graph(fallers, measure = "Consumer Prices", item = "item_name",
                                time_comparison = x, data_freq = "monthly",
                                geo_level = "state", caption = cpi_caption)
           
    })
  
  })
  
  
},
error = function(e) {
  e_full <- error_cnd(class = "consumer_prices_error", message = paste(
    "An error occured with the consumer prices update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

gas_prices <- tryCatch({
  
  # Weekly gas in Tri-state area from the EIA
  # https://www.eia.gov/petroleum/gasdiesel/
  download.file(url = "https://www.eia.gov/petroleum/gasdiesel/xls/pswrgvwall.xls",
                destfile = "./reference_files/eia_gas_prices.xls")
  
  read_excel(path = "./reference_files/eia_gas_prices.xls", col_names = T, col_types = "text",
             skip = 2, sheet = "Data 3") %>% 
    rename_with(.fn = make_clean_names) %>% 
    mutate(date = base::as.Date(as.integer(date), origin = "1899-12-30")) %>% 
    mutate(across(.cols = !date, .fns = as.double)) -> full_gas_eia
  
  full_gas_eia %>% 
    select(date, contains(c( "central_atlantic", "new_england", "new_york"))) %>% 
    filter(date >= base::as.Date("2019-01-01")) %>% 
    rename(`Central Atlantic` = weekly_central_atlantic_padd_1b_regular_all_formulations_retail_gasoline_prices_dollars_per_gallon,
           `New England` = weekly_new_england_padd_1a_regular_all_formulations_retail_gasoline_prices_dollars_per_gallon,
           `New York State` = weekly_new_york_regular_all_formulations_retail_gasoline_prices_dollars_per_gallon,
           `New York City` = weekly_new_york_city_regular_all_formulations_retail_gasoline_prices_dollars_per_gallon) %>% 
    pivot_longer(!date, names_to = "state", values_to = "value") %>% 
    mutate(value = round(value, 2),
           month = rollback(date, roll_to_first = T)) %>% 
    left_join(cpi_inf, by = c("month" = "date")) %>% 
    mutate(
      inf_pct = if_else(is.na(inf_pct), 1, inf_pct),
      value = value * inf_pct
      ) %>% 
    select(-c(inf_pct, month)) -> gas_tri_state
  
  write_csv(gas_tri_state, "./data/gas_prices_long.csv")
  
  gas_tri_state %>% 
    pivot_wider(names_from = state, values_from = value) %>% 
    write_csv("./data/gas_prices_wide.csv")
  
  ## Visualizations
  # By region line graph over time
  time_series_line_graph(df = gas_tri_state,
                         measure = "Gas Prices",
                         data_freq = "daily",
                         value_format = "dollar",
                         geo_level = "state",
                         item = "state",
                         caption = paste(gas_price_caption,
                                         inf_caption, 
                                         sep = "\n"))
  
  # By region WoW, MoM, YoY, 2019 - present
  walk(c("Week-over-week", time_frames), function(x) {
    gas_tri_state %>% 
      change_time_bar_calc(time_change = x, level = "state", weekly = T) %>% 
      comparison_bar_graph(measure = "Gas Prices",
                           time_comparison = x,
                           data_freq = "weekly",
                           geo_level = "state",
                           item = "state",
                           caption = paste(gas_price_caption,
                                           inf_caption, 
                                           sep = "\n"))
    
  })
  
  
},
error = function(e) {
  e_full <- error_cnd(class = "gas_prices_error", message = paste(
    "An error occured with the gas prices update:", 
    e, "on", Sys.Date(), "\n"))
  
  message(e_full[["message"]])
  
  return(e_full)
}
)

Sys.sleep(3)

wages <- tryCatch({
  
  # Industry code file
  read_delim(file = "https://download.bls.gov/pub/time.series/sm/sm.industry",
             delim = "\t", trim_ws = T, col_names = T,
             col_types = "cc") -> industry_codes
  
  # Wages by state and sector from the BLS
  read_delim(file = "https://download.bls.gov/pub/time.series/sm/sm.data.1.AllData",
             delim = "\t", trim_ws = T, col_names = T,
             col_types = "cccdc") %>% 
    filter(as.integer(year) >= 2019, period != "M13") %>% 
    mutate(area = str_sub(series_id, 4, 10),
           industry_code = str_sub(series_id, 11, 18),
           seasonal_adjustment = str_sub(series_id, 3, 3),
           data_type = str_sub(series_id, 19, 20),
           date = base::as.Date(paste0(year, "-", str_remove(period, "M"), "-01")),
           state = case_when(area == "0900000" ~ "Connecticut",
                             area == "3400000" ~ "New Jersey",
                             area == "3600000" ~ "New York")
           ) %>% 
    filter(area %in% c("0900000", "3400000", "3600000"),
           data_type == "03", seasonal_adjustment == "U") %>% 
    select(date, series_id, state, industry_code, value) %>% 
    inner_join(industry_codes, by = "industry_code")  %>% 
    left_join(cpi_inf, by = "date") %>% 
    mutate(inf_pct = if_else(is.na(inf_pct), 1, inf_pct),
           value = value * inf_pct) %>% 
    select(-inf_pct) -> wages_tri_state
  
  Sys.sleep(5)
  
  write_csv(wages_tri_state, "./data/hourly_wages_long.csv")
  
  wages_tri_state %>% 
    select(-c(series_id, industry_code)) %>% 
    pivot_wider(names_from = industry_name, values_from = value) %>% 
    write_csv("./data/hourly_wages_wide.csv")
  
  ## Visualizations
  
  # Line graph by state & industry over time
  wages_tri_state %>% 
    group_split(industry_name) %>% 
    walk(~time_series_line_graph(df = .x,
                         measure = "Hourly Wages",
                         data_freq = "monthly",
                         value_format = "dollar",
                         geo_level = "state",
                         item = "state",
                         caption = paste(bls_caption,
                                         inf_caption,
                                         sep = "\n")
                         ))
  
  # By state bar graph MoM, YoY, and 2019-present percent changes (top four)
  
  walk(time_frames, function(x) {
    
    wages_tri_state %>% 
      group_split(state) %>% 
      map(~change_time_bar_calc(df = .x, time_change = x, level = "industry_name")) %>% 
      walk(function(y) {
        
        risers <- slice_max(y, order_by = pct, n = 4) %>% 
          mutate(direction = "gains")
        fallers <- slice_min(y, order_by = pct, n = 4) %>% 
          mutate(direction = "losses")
        
        comparison_bar_graph(risers, measure = "Hourly Wages", item = "industry_name",
                             time_comparison = x, data_freq = "monthly",
                             geo_level = "state", caption = paste(bls_caption,
                                                                  inf_caption,
                                                                  sep = "\n"))
        
        comparison_bar_graph(fallers, measure = "Hourly Wages", item = "industry_name",
                             time_comparison = x, data_freq = "monthly",
                             geo_level = "state", caption = paste(bls_caption,
                                                                  inf_caption,
                                                                  sep = "\n"))
        
      })
    
  })
  
  
})


