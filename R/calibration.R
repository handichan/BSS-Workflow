<<<<<<< HEAD

setwd("R")
#install the packages if they're not already installed
packages <- c("tidyverse", "scales", "cowplot", "maps", "mapdata", "colorspace")
install.packages(setdiff(packages, rownames(installed.packages())))

library(tidyverse)
library(scales)
library(cowplot)
library(maps)
library(mapdata)
library(colorspace)

theme_set(theme_bw())

=======
>>>>>>> 7538764818adf826b977c473280da6473d312944
# uncalibrated BSS monthly results
# use a version of long_county_hourly_ that has 2020-2024 to run the following query state_monthly.sql)
# save the result to diagnostics/state_monthly_for_cal.csv


eia_gross<-read_csv("../map_meas/eia_gross_consumption_by_state_sector_year_month.csv")
state_monthly<-read_csv("../diagnostics/state_monthly_for_cal.csv")

# if you have TMY
state_monthly_tmy<-read_csv("../diagnostics/state_monthly_for_cal_tmy.csv") %>%
  mutate(kind="state_monthly_tmy_kwh") %>% rename("kwh"="state_monthly_tmy_kwh")

type_label<-c(state_monthly_uncal_kwh="BSS uncalibrated",state_monthly_cal_kwh="BSS calibrated",state_monthly_tmy_kwh="BSS uncalibrated, TMY weather")
s_label<-c(com="Commercial",res="Residential",all="Buildings")

# calculate monthly state-level calibration ratios ------------------------
monthly_ratios<-state_monthly %>%
  inner_join(eia_gross,by=c("in.state","month","sector","year")) %>%
  mutate(gross_over_bss=gross.kWh/state_monthly_uncal_kwh,
         net_over_bss=sales.kWh/state_monthly_uncal_kwh) %>% 
  group_by(sector, in.state, month) %>%
  summarize(calibration_multiplier=mean(gross_over_bss),.groups="drop")

write_tsv(monthly_ratios, "../map_meas/calibration_multipliers.tsv")



# post calibration: state level combined bar and line plots -------------------------------------------

bss<-state_monthly %>% full_join(monthly_ratios,by=c("in.state","month","sector")) %>%
  mutate(state_monthly_cal_kwh=state_monthly_uncal_kwh*calibration_multiplier) %>% 
  select(-calibration_multiplier) %>% 
  pivot_longer(names_to="type",values_to="kwh",state_monthly_uncal_kwh:state_monthly_cal_kwh)

if(exists(state_monthly_tmy)){
  bss<-bind_rows(bss,state_monthly_tmy)
}


for (st in c(state.abb[!(state.abb %in% c("AK","HI"))],"DC")) {
  for (sct in c("res","com")){
    eia_gross %>% 
      filter(in.state==st,sector==sct,year %in% 2020:2024) %>%
      ggplot(aes(x=month))+
      geom_col(aes(y=gross.kWh,fill="EIA gross"))+
      geom_line(data=bss %>% filter(in.state==st,sector==sct,year %in% 2020:2024),
                aes(y=kwh,color=type),linewidth=1.5)+
      scale_x_continuous(name="Month",breaks=seq(1,12,by=3))+
      scale_y_continuous(name="kWh",expand = expansion(mult=c(0,.05),add=0))+
      scale_color_brewer(name="",labels=type_label,palette = "Set1")+
      scale_fill_manual(name="",values="gray70")+
      facet_wrap(~year)+
      ggtitle(paste(st,sct))

    ggsave(paste0("graphs/calibration graphs/",st,"_",sct,".jpg"),
           device = "jpeg",width = 10, height =6,units = "in")
  }
}

# post calibration: compare state-level seasonal ratios to EIA ------------------------------

eia_ratios_sector<-eia_gross %>% filter(sector %in% c("res","com")) %>%
  group_by(in.state,year,season,sector) %>% summarize(gross.kWh_max=max(gross.kWh)) %>%
  pivot_wider(names_from=season,values_from=c(gross.kWh_max)) %>%
  mutate(max_winter_to_max_summer=Winter/Summer)

bss_ratios_sector<-bss %>%
  mutate(season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(in.state,year,sector,season,type) %>% summarize(monthly_max=max(kwh)) %>%
  pivot_wider(names_from=season,values_from=monthly_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer) %>%
  arrange(max_winter_to_max_summer)


quads<-eia_ratios_sector %>% select(in.state,year,sector,eia_ratio=max_winter_to_max_summer) %>%
  inner_join(bss_ratios_sector %>% select(in.state,year,sector,type,bss_ratio=max_winter_to_max_summer),
             by=c("in.state","year","sector")) %>%
  mutate(peak_diff=case_when(eia_ratio>1 & bss_ratio>1 ~ "Both winter",
                             eia_ratio>1 & bss_ratio<1 ~ "EIA winter",
                             eia_ratio<1 & bss_ratio>1 ~ "BSS winter",
                             eia_ratio<1 & bss_ratio<1 ~ "Both summer",
                             TRUE ~ "other")) 
quads %>%
  ggplot(aes(x=eia_ratio,y=bss_ratio,label=in.state,color=peak_diff))+
  geom_abline(slope=1)+
  geom_text()+
  scale_colour_manual(values=c("#E69F00", #orange
                               "#56B4E9", #blue 
                               "#009E73", #green,
                               "#CC79A7" #magenta
  ),name="Season with peak month")+
  xlab("EIA")+
  ylab("BSS")+
  facet_grid(sector+type~year,labeller = labeller(sector=s_label,type=type_label))+
  ggtitle("Max monthly buildings electricity consumption: ratio of winter to summer")



# v1 method ---------------------------------------------------------------

# optimization model
# state-level multipliers on heating, cooling, and everything else
# keep total constant, match winter to summer ratio
# get passed to Scout; no more calibration after that


# state, month, end use consumption from BSS disaggregation
# smeu<-read_csv("")

# library(optim)
# 
# 
# # Define penalty weights for equality constraints
# penalty_weight <- 100
# 
# # Modified objective function with penalties for constraints
# objective_with_constraints_floating <- function(m) {
#   m_heat <- m[1]
#   m_cool <- m[2]
#   m_base <- m[3]
#   
#   # Modeled total with multipliers
#   modeled_total <- modeled_heating * m_heat + modeled_cooling * m_cool + modeled_base * m_base
#   
#   # Objective: Minimize squared differences
#   squared_diff <- sum((modeled_total - measured_data)^2)
#   
#   # Constraint 1: Annual total remains constant
#   annual_total_diff <- 1-sum(modeled_total) / annual_total_modeled
#   
#   # Constraint 2: Winter-to-summer ratio matches measured
#   winter_max_modeled <- modeled_total[win_month]
#   summer_max_modeled <- modeled_total[sum_month]
#   ratio_diff <- (winter_max_modeled / summer_max_modeled) - winter_summer_ratio_measured
#   
#   # Add penalties for constraint violations
#   penalty <- penalty_weight * ratio_diff^2
#   
#   # Total objective with penalties
#   squared_diff + penalty
# }
# 
# # Initial guesses for multipliers
# initial_guess <- c(1, 1, 1)
# 
# 
# # results_floating<-data.frame()
# 
# for (st in c(state.abb[!(state.abb %in% c("AK","HI"))],"DC")) {
#   # Define the data
#   measured_data <- (eia_res_month %>% filter(in.state==st,year==2018))$sales.kWh  # Monthly measured electricity
#   sum_month<- (eia_res_month %>% filter(in.state==st,year==2018,season=="Summer") %>% arrange(desc(sales.kWh)))[1,]$month_number
#   win_month<- (eia_res_month %>% filter(in.state==st,year==2018,season=="Winter") %>% arrange(desc(sales.kWh)))[1,]$month_number
#   modeled_heating <- (smeu %>% filter(in.state==st,year==2018,end_use=="Heating (Equip.)",sector=="res"))$state_monthly_kwh  # Monthly modeled heating
#   modeled_cooling <- (smeu %>% filter(in.state==st,year==2018,end_use=="Cooling (Equip.)",sector=="res"))$state_monthly_kwh  # Monthly modeled cooling
#   modeled_base <- (smeu %>% filter(in.state==st,year==2018,!(end_use %in% c("Heating (Equip.)","Cooling (Equip.)")),sector=="res") %>%
#                      group_by(month) %>% summarize(non=sum(state_monthly_kwh)))$non     # Monthly modeled base electricity
#   
#   # Calculate constants
#   annual_total_modeled <- sum(modeled_heating + modeled_cooling + modeled_base)
#   winter_max_measured <- measured_data[win_month] #max(measured_data[c(1:2,11:12)])
#   summer_max_measured <- measured_data[sum_month] #max(measured_data[5:9])
#   winter_summer_ratio_measured <- winter_max_measured / summer_max_measured
#   
#   
#   # Run the optimization
#   result <- optim(
#     par = initial_guess,
#     fn = objective_with_constraints_floating,
#     method = "L-BFGS-B", # Bounded optimization
#     lower = c(-1, -1, -1), # Lower bounds for multipliers (set as appropriate)
#     upper = c(20, 20, 20)        # Upper bounds for multipliers (set as appropriate)
#   )
#   
#   # Extract results
#   multipliers <- result$par
#   names(multipliers) <- c("m_heat", "m_cool", "m_base")
#   multipliers
#   
#   results_floating<-bind_rows(results_floating,c(in.state=st,multipliers))
# }
# 
# results_floating<-results_floating %>% mutate(m_heat=as.numeric(m_heat),m_cool=as.numeric(m_cool),m_base=as.numeric(m_base))
# 
# 
# #state monthly end use with the multipliers applied
# smeu <- smeu %>% filter(sector=="res",year==2018) %>%
#   full_join(results_floating,by=c("in.state")) %>%
#   mutate(monthly_adj=case_when(end_use == "Heating (Equip.)" ~ m_heat * state_monthly_kwh,
#                                end_use == "Cooling (Equip.)" ~ m_cool * state_monthly_kwh,
#                                TRUE ~ m_base * state_monthly_kwh))
