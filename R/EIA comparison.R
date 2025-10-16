setwd("R")

#install the packages if they're not already installed
packages <- c("tidyverse", "scales", "cowplot")
install.packages(setdiff(packages, rownames(installed.packages())))

library(tidyverse)
library(scales)
library(cowplot)

theme_set(theme_bw())

input_dir <- "../generated_csvs" #directory where the csvs are stored
filename_prefix <- ""
graph_dir <- "graphs" #directory where the graphs will be written

state_monthly<-read_csv(paste0(input_dir,"/",filename_prefix,"aeo_state_monthly.csv"))

# compare state-level seasonal ratios to EIA ------------------------------

eia<-read_csv("../map_meas/eia_gas_and_electricity_by_state_sector_year_month.csv")

eia_ratios_sector<-eia %>% filter(fuel=="electricity",sector %in% c("residential","commercial")) %>% 
  mutate(month=(match(month, month.abb)),
         season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder"),
         in.state=if_else(state=="District of Columbia","DC",state.abb[match(state,state.name)]),
         sector=if_else(sector=="commercial","com","res")) %>%
  group_by(state,in.state,year,month,season,sector) %>% summarize(sales.kWh=sum(sales.kWh)) %>%
  group_by(state,in.state,year,season,sector) %>% summarize(sales.kWh_max=max(sales.kWh)) %>%
  pivot_wider(names_from=season,values_from=sales.kWh_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer)

bss_ratios_sector<-state_monthly %>%
  mutate(season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(in.state,sector,season) %>% summarize(monthly_max=max(state_monthly_kwh)) %>%
  pivot_wider(names_from=season,values_from=monthly_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer) %>%
  arrange(max_winter_to_max_summer)

eia_comp<-eia_ratios_sector %>%  filter(year<2024) %>%
  ggplot(aes(x=in.state,y=max_winter_to_max_summer))+
  geom_hline(yintercept = 1,color="grey30")+
  geom_boxplot()+
  geom_point(data=bss_ratios_sector ,color="red")+
  scale_x_discrete(limits=bss_ratios_sector[bss_ratios_sector$sector=="res",]$in.state,name="")+
  ylab("winter max / summer max")+
  facet_wrap(~sector,nrow=2,labeller = labeller(sector=s_label))+
  ggtitle("Ratio of max monthly electricity consumption in the winter max to max monthly electricity consumption in the summer",subtitle = "EIA 861 2001-2023 (boxplot) vs. BSS 2024 baseline (red dot)")
save_plot(paste0(graph_dir,"/",filename_prefix,"/eia_seasonal_ratio_comp.jpg"),eia_comp,base_height = 6,base_width = 12,bg="white")
