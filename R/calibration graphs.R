
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

eia_gross<-read_csv("../map_meas/eia_gross_consumption_by_state_sector_year_month.csv")
state_monthly<-read_csv("generated_csvs/aeo_010926_state_monthly.csv")

type_label<-c(state_monthly_uncal_kwh="BSS uncalibrated",state_monthly_cal_kwh="BSS calibrated",state_monthly_tmy_kwh="BSS uncalibrated, TMY weather")
s_label<-c(com="Commercial",res="Residential",all="Buildings")


# post calibration: state level combined bar and line plots -------------------------------------------

bss<-state_monthly %>% 
  pivot_longer(names_to="type",values_to="kwh",state_monthly_uncal_kwh:state_monthly_cal_kwh)

for (st in c(state.abb[!(state.abb %in% c("AK","HI"))],"DC")) {
  for (sct in c("res","com")){
    eia_gross %>% 
      filter(in.state==st,sector==sct,year %in% 2020:2024) %>%
      ggplot(aes(x=month))+
      geom_col(aes(y=gross.kWh,fill="EIA gross"))+
      geom_line(data=bss %>% filter(in.state==st,sector==sct,year %in% 2020:2024),
                aes(y=kwh,color=type,linetype=type),linewidth=1.25)+
      scale_x_continuous(name="Month",breaks=seq(1,12,by=3))+
      scale_y_continuous(name="kWh",expand = expansion(mult=c(0,.05),add=0))+
      scale_color_brewer(name="",labels=type_label,palette = "Dark2",limits=c("state_monthly_cal_kwh","state_monthly_uncal_kwh"))+
      scale_fill_manual(name="",values="gray70")+
      # scale_linetype(guide="none")+
      scale_linetype(name="",labels=type_label,limits=c("state_monthly_cal_kwh","state_monthly_uncal_kwh"))+
      facet_grid(fuel~year,scales="free_y")+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=0,size=10),strip.text.x = element_text(size=10))+
      ggtitle(paste(st,sct))
    ggsave(paste0("graphs/calibration graphs/",st,"_",sct,".jpg"),
           device = "jpeg",width = 10, height =4,units = "in")
  }
}

# post calibration: compare state-level seasonal ratios to EIA ------------------------------

eia_ratios_sector<-eia_gross %>% filter(sector %in% c("res","com")) %>%
  group_by(in.state,year,season,sector,fuel) %>% summarize(gross.kWh_max=max(gross.kWh)) %>%
  pivot_wider(names_from=season,values_from=c(gross.kWh_max)) %>%
  mutate(max_winter_to_max_summer=Winter/Summer)

bss_ratios_sector<-bss %>%
  mutate(season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(in.state,year,sector,season,type,fuel) %>% summarize(monthly_max=max(kwh)) %>%
  pivot_wider(names_from=season,values_from=monthly_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer) %>%
  arrange(max_winter_to_max_summer)


quads<-eia_ratios_sector %>% select(in.state,year,sector,fuel,eia_ratio=max_winter_to_max_summer) %>%
  inner_join(bss_ratios_sector %>% select(in.state,year,sector,fuel,type,bss_ratio=max_winter_to_max_summer),
             by=c("in.state","year","sector","fuel")) %>%
  mutate(peak_diff=case_when(eia_ratio>1 & bss_ratio>1 ~ "Both winter",
                             eia_ratio>1 & bss_ratio<1 ~ "EIA winter",
                             eia_ratio<1 & bss_ratio>1 ~ "BSS winter",
                             eia_ratio<1 & bss_ratio<1 ~ "Both summer",
                             TRUE ~ "other")) 
quadgraph_elec<-quads %>%
  filter(type!="state_monthly_tmy_kwh",fuel=="Electric") %>%
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

quadgraph_gas<-quads %>%
  filter(type!="state_monthly_tmy_kwh",fuel=="Natural Gas") %>%
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
  scale_x_continuous(name="EIA-861M",limits=c(.4,15))+
  scale_y_continuous(name="BSS",limits=c(.4,15))+
  facet_grid(sector+type~year,labeller = labeller(sector=s_label))+
  ggtitle("Max monthly buildings natural gas consumption: ratio of winter to summer")+
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