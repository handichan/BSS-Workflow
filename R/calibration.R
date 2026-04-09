if (basename(getwd()) != "R") {
    setwd("R")
}
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

# uncalibrated BSS monthly results
# use a version of long_county_hourly_ that has 2020-2024 to run state_monthly.sql)
# save the result to diagnostics/state_monthly_for_cal.csv


eia_gross<-read_csv("../map_meas/eia_gross_consumption_by_state_sector_year_month.csv")
state_monthly<-read_csv("../diagnostics/state_monthly_for_cal.csv")

# if you have TMY
## got from https://drive.google.com/open?id=1bDdEzD19vjXzObCdo6lQNtBOfyHBBG23&usp=drive_fs
## rename to state_monthly_for_cal_tmy.csv
if (file.exists("../diagnostics/state_monthly_for_cal_tmy.csv")) {
  state_monthly_tmy<- read_csv("../diagnostics/state_monthly_for_cal_tmy.csv") %>%
  mutate(type="state_monthly_tmy_kwh") %>%
  rename("kwh"="state_monthly_kwh") %>%
  {.}
}

type_label<-c(state_monthly_uncal_kwh="BSS uncalibrated",state_monthly_cal_kwh="BSS calibrated",state_monthly_tmy_kwh="BSS uncalibrated, TMY weather")
s_label<-c(com="Commercial",res="Residential",all="Buildings")

# calculate monthly state-level calibration ratios ------------------------
monthly_ratios<-state_monthly %>%
  dplyr::mutate_at(vars(month, year), as.numeric) %>%
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

if(exists("state_monthly_tmy")){
  bss<-bind_rows(bss,state_monthly_tmy)
}

## used in p61 of reference slide deck
years = c(2020, 2022, 2024)

for (st in c(state.abb[!(state.abb %in% c("AK","HI"))],"DC")) {
  for (sct in c("res","com")){
    eia_gross %>%
      filter(in.state==st,sector==sct,year %in% years) %>%
      ggplot(aes(x=month))+
      geom_col(aes(y=gross.kWh,fill="EIA gross"))+
      geom_line(data=bss %>% filter(in.state==st,sector==sct,year %in% years),
                aes(y=kwh,color=type,linetype=type),linewidth=1.5)+
      scale_x_continuous(name="Month",breaks=seq(1,12,by=3))+
      scale_y_continuous(name="kWh",expand = expansion(mult=c(0,.05),add=0))+
      scale_color_brewer(name="",labels=type_label,palette = "Dark2",limits=c("state_monthly_cal_kwh", "state_monthly_tmy_kwh", "state_monthly_uncal_kwh"))+
      scale_fill_manual(name="",values="gray70")+
      scale_linetype(guide="none")+
      facet_wrap(~year)+
      theme_bw() +
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=0,size=10),strip.text.x = element_text(size=10))+
      ggtitle(paste(st,sct))
    ggsave(paste0("graphs/calibration graphs/",st,"_",sct,"_3year.jpg"),
           device = "jpeg",width = 10, height =4,units = "in")
  }
}

## used in paper
years = c(2020, 2022, 2024)
eia_gross %>%
  dplyr::filter(in.state %in% c("CO", "FL", "SD"), sector=="res", year %in% years) %>%
  ggplot(aes(x=month))+
  geom_col(aes(y=gross.kWh,fill="EIA gross"))+
  geom_line(data=bss %>% filter(in.state %in% c("CO", "FL", "SD"), sector=="res", year %in% years),
            aes(y=kwh,color=type,linetype=type),linewidth=1.5)+
  scale_x_continuous(name="Month",breaks=seq(1,12,by=3))+
  scale_y_continuous(name="kWh",expand = expansion(mult=c(0,.05),add=0))+
  scale_color_brewer(name="",labels=type_label,palette = "Dark2",limits=c("state_monthly_cal_kwh", "state_monthly_tmy_kwh", "state_monthly_uncal_kwh"))+
  scale_fill_manual(name="",values="gray70")+
  scale_linetype(guide="none")+
  facet_grid(in.state~year, scales = "free_y") +
  theme_minimal() +
  theme(panel.border = element_rect(color = "black", fill = NA))
ggsave(paste0("graphs/calibration graphs/","fig_res_cal_SD_CO_FL.jpg"),
           device = "jpeg",width = 10, height =6,units = "in")

years = 2020:2024
for (st in c(state.abb[!(state.abb %in% c("AK","HI"))],"DC")) {
  for (sct in c("res","com")){
    eia_gross %>% 
      filter(in.state==st,sector==sct,year %in% years) %>%
      ggplot(aes(x=month))+
      geom_col(aes(y=gross.kWh,fill="EIA gross"))+
      geom_line(data=bss %>% filter(in.state==st,sector==sct,year %in% years),
                aes(y=kwh,color=type,linetype=type),linewidth=1.5)+
      scale_x_continuous(name="Month",breaks=seq(1,12,by=3))+
      scale_y_continuous(name="kWh",expand = expansion(mult=c(0,.05),add=0))+
      scale_color_brewer(name="",labels=type_label,palette = "Dark2",limits=c("state_monthly_uncal_kwh","state_monthly_tmy_kwh","state_monthly_cal_kwh"))+
      scale_fill_manual(name="",values="gray70")+
      scale_linetype(guide="none")+
      facet_wrap(~year)+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=0,size=10),strip.text.x = element_text(size=10))+
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
quadgraph<-quads %>%
  filter(type!="state_monthly_tmy_kwh") %>%
  mutate(type=factor(type,levels=c("state_monthly_uncal_kwh","state_monthly_cal_kwh"),ordered=T,labels=c("Uncalibrated","Calibrated")))%>%
  ggplot(aes(x=eia_ratio,y=bss_ratio,label=in.state,color=peak_diff))+
  geom_abline(slope=1)+
  geom_hline(yintercept=1,color="gray80")+
  geom_vline(xintercept=1,color="gray80")+
  geom_point(size=1)+
  scale_colour_manual(values=c("#E69F00", #orange
                               "#56B4E9", #blue 
                               "#009E73", #green,
                               "#CC79A7" #magenta
  ),name="Season with peak month",guide="none")+
  scale_x_continuous(name="EIA-861M",limits=c(.4,1.9))+
  scale_y_continuous(name="BSS",limits=c(.4,1.9))+
  facet_grid(sector+type~year,labeller = labeller(sector=s_label))+
  ggtitle("Max monthly buildings electricity consumption: ratio of winter to summer")+
  theme(aspect.ratio = 1,strip.background = element_blank(),strip.text.y = element_text(size=10),strip.text.x = element_text(size=10))

leg<-data.frame(x=c(1,4,1,4),y=c(1,4,4,1),label=c("Both summer","Both winter","Disagree:
BSS winter
EIA summer","Disagree:
EIA winter
BSS summer")) %>%
  ggplot(aes(x=x,y=y))+
  geom_tile(fill="white",color="gray50")+
  geom_text(aes(label=label,color=label),hjust=.5,fontface="bold")+
  scale_color_manual(values=c("#E69F00", #orange
                              "#56B4E9", #blue 
                              "#009E73", #green,
                              "#CC79A7" #magenta
                              ),guide="none")+
  theme_void()+
  theme(aspect.ratio = 1)

save_plot(plot_grid(quadgraph,leg,nrow = 1,rel_widths = c(4,1)),filename = "graphs/fig_max_ratios.jpg",base_height = 7,bg = "white")

quads %>%
  filter(type!="state_monthly_tmy_kwh") %>%
  filter(stringr::str_detect(type, "_uncal_")) %>%
  ggplot(aes(x=eia_ratio,y=bss_ratio,label=in.state,color=peak_diff))+
  geom_abline(slope=1)+
  geom_hline(yintercept=1,color="gray80")+
  geom_vline(xintercept=1,color="gray80")+
  geom_point(size=1)+
  scale_colour_manual(values=c("#E69F00", #orange
                               "#56B4E9", #blue 
                               "#009E73", #green,
                               "#CC79A7" #magenta
                               ),name="Season with peak month",guide="none")+
  geom_text() +
  scale_x_continuous(name="EIA",limits=c(.4,1.9))+
  scale_y_continuous(name="BSS,uncalibrated",limits=c(.4,1.9))+
  facet_grid(sector~year,labeller = labeller(sector=s_label))+
  ## ggtitle("Max monthly buildings electricity consumption: ratio of winter to summer")+
  theme(aspect.ratio = 1.0,strip.background = element_blank(),strip.text.y = element_text(size=10),strip.text.x = element_text(size=10)) +
  theme_bw()
  ggsave("graphs/uncalib_fig_max_ratios.jpg", width = 9, height = 4.5)

quads %>%
  filter(type!="state_monthly_tmy_kwh") %>%
  filter(stringr::str_detect(type, "_cal_")) %>%
  ggplot(aes(x=eia_ratio,y=bss_ratio,label=in.state,color=peak_diff))+
  geom_abline(slope=1)+
  geom_hline(yintercept=1,color="gray80")+
  geom_vline(xintercept=1,color="gray80")+
  geom_point(size=1)+
  scale_colour_manual(values=c("#E69F00", #orange
                               "#56B4E9", #blue 
                               "#009E73", #green,
                               "#CC79A7" #magenta
                               ),name="Season with peak month",guide="none")+
  geom_text() +
  scale_x_continuous(name="EIA",limits=c(.4,1.9))+
  scale_y_continuous(name="BSS,calibrated",limits=c(.4,1.9))+
  facet_grid(sector~year,labeller = labeller(sector=s_label))+
  ## ggtitle("Max monthly buildings electricity consumption: ratio of winter to summer")+
  theme(aspect.ratio = 1,strip.background = element_blank(),strip.text.y = element_text(size=10),strip.text.x = element_text(size=10)) +
  theme_bw()
  ggsave("graphs/calib_fig_max_ratios.jpg", width = 9, height = 4.5)

## monthly calibration factor plot
monthly_ratios %>%
  ggplot2::ggplot(ggplot2::aes(x = month, y = calibration_multiplier, color = sector)) +
  ggplot2::geom_line() +
  ggplot2::facet_wrap(.~in.state, ncol=7, scales = "free_y") +
  ggplot2::geom_hline(yintercept = 1) +
  scale_x_continuous(name = "Month", breaks = seq(1, 12, by = 3), labels = function(x) as.integer(x)) +
  ggplot2::theme_bw()
ggsave("graphs/calibration graphs/calib_mult_by_month.jpg", width = 11, height = 9)

## used in p62 of reference slide deck
monthly_ratios %>%
  dplyr::filter(sector == "res") %>%
  dplyr::select(calibration_multiplier) %>%
  summary()

monthly_ratios %>%
  dplyr::filter(sector == "com") %>%
  dplyr::select(calibration_multiplier) %>%
  summary()


p1 <- bss %>%
  dplyr::group_by(turnover, sector, in.state, type) %>%
  dplyr::mutate_at(vars(sector), recode, "res"="Residential", "com"="Commercial") %>%
  dplyr::summarise(kwh = sum(kwh) / 5) %>%
  dplyr::ungroup() %>%
  tidyr::spread(type, kwh) %>%
  dplyr::mutate(percent.increase = (state_monthly_cal_kwh - state_monthly_uncal_kwh) / state_monthly_uncal_kwh) %>%
  ggplot2::ggplot(aes(x = in.state, y = percent.increase)) +
  ggplot2::geom_bar(stat="identity") +
  ggplot2::facet_wrap(sector~., ncol = 1, strip.position = "right") +
  ggplot2::scale_y_continuous(labels = percent_format()) +
  ggplot2::theme_minimal() +
  ggplot2::ggtitle("Percent increase in annual electricity compared to uncalibrated") +
  theme(panel.border = element_rect(color = "black", fill = NA),
        axis.title.x = element_blank(),axis.title.y = element_blank())

df.state.to.census <- tibble::tibble(in.state = c(state.abb, "DC"), census_div = c(state.division, state.division[[8]]))

p2 <- bss %>%
  dplyr::mutate_at(vars(sector), recode, "res"="Residential", "com"="Commercial") %>%
  dplyr::left_join(df.state.to.census, by="in.state") %>%
  dplyr::group_by(turnover, sector, census_div, type) %>%
  dplyr::summarise(kwh = sum(kwh) / 5) %>%
  dplyr::ungroup() %>%
  tidyr::spread(type, kwh) %>%
  dplyr::mutate(percent.increase = (state_monthly_cal_kwh - state_monthly_uncal_kwh) / state_monthly_uncal_kwh) %>%
  ggplot2::ggplot(ggplot2::aes(x = census_div, y = percent.increase)) +
  ggplot2::geom_bar(stat="identity") +
  ggplot2::facet_wrap(sector~., nrow = 1) +
  ggplot2::scale_y_continuous(labels = percent_format()) +
  ggplot2::theme_minimal() +
  # fixme change xtick label to 45 deg
  ggplot2::theme(axis.text.x = element_text(angle = 45, hjust = 1),
                 panel.border = element_rect(color = "black", fill = NA),
                 axis.title.x = element_blank(),axis.title.y = element_blank())

library("cowplot")

plots <- align_plots(p1, p2, align = "v", axis = "l")  # align vertically on left axis
cowplot::plot_grid(plots[[1]], plots[[2]], ncol = 1)
ggplot2::ggsave("graphs/calibration graphs/fig_ann_cal_uncal.png", width = 13, height = 8)

# restore previous base directory
setwd("../")
