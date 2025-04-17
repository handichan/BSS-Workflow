# initialize ---------------------------------------------------------------------
setwd("R")

#install the packages if they're not already installed
packages <- c("tidyverse", "scales")
install.packages(setdiff(packages, rownames(installed.packages())))

#load required packages
library(tidyverse)
library(scales)
theme_set(theme_bw())

# Scout results - output of calc_annual 
wide<-data.frame()
for (file in list.files("../scout_results")){
  if (nrow(wide)==0) {
    wide<-bind_rows(wide,read_tsv(paste0("../scout_results/",file)))
  } else if(nrow(wide %>% filter(turnover=="baseline"))==0){
    wide<-bind_rows(wide,read_tsv(paste0("../scout_results/",file)))
  }else{
    wide<-bind_rows(wide,read_tsv(paste0("../scout_results/",file)) %>% filter(turnover!="baseline"))
  }
}
wide<-wide %>%
  mutate(sector=if_else(str_detect(meas,"\\(R\\)"),"res","com")) %>%
  filter(!is.na(sector))


# nice names for the Scout measures
mm<-read_tsv("../map_meas/measure_map.tsv")
mm_long<-pivot_longer(mm %>% select(-c(original_ann:measure_ts)) %>% rename(measure_ann=measure_desc_simple,original_ann=original_desc_simple),
                      names_to="tech_stage",values_to="description",original_ann:measure_ann)



# for nice labeling
# Scout scenarios -- every value of "turnover" should be here
to<-c(baseline="Reference",breakthrough="Breakthrough",ineff="Inefficient",high="High",mid="Mid",stated_policies="Stated Policies")
# sector
s<-c(com="Commercial",res="Residential",all="All Buildings")
# end uses
eu<-c(`Computers and Electronics`="Computers and Electronics",Cooking="Cooking",`Cooling (Equip.)`="Cooling",`Heating (Equip.)`="Heating",Lighting="Lighting",Other="Other",Refrigeration="Refrigeration",Ventilation="Ventilation",`Water Heating`="Water Heating")

#fill colors
colors<-c("#e41a1c","#fbb4ae","#377eb8","#b3cde3","#4daf4a",
          "#ccebc5","#984ea3","#decbe4","#ff7f00","#fed9a6",
          "#ffee33","#ffffcc","#a65628","#e5d8bd","#f781bf",
          "#fddaec","#999999","#f2f2f2",
          "#5b92e5","#d4a017", "#8fbc8f","#ff69b4")
#width changeable based on number of scenarios
width<-(1+length(unique(wide$turnover)))*1.8


# states to show as examples for heating and cooling
states<-c("WA","CA","MA","FL")
state_height<-length(states)*1.4

# annual, national --------------------------------------------------------

print("printing 1")
# area plot by year, scenario, end use, sector
wide %>% filter(fuel=="Electric") %>% 
  group_by(year,sector,end_use,turnover) %>% 
  summarize(kwh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=kwh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=s))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors)+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
ggsave("graphs/national_annual_sector_scenario.jpeg",device = "jpeg",width = width, height =3,units = "in")


print("printing 2")
# line plot comparing the scenario totals
wide %>% filter(fuel=="Electric") %>%
  group_by(turnover,year) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=TWh,color=turnover))+
  geom_line()+
  scale_y_continuous(limits=c(0,NA),labels=comma_format())+
  scale_color_manual(values =colors,name="",labels=to)+
  xlab("")
ggsave("graphs/national_annual_lines.jpeg",device = "jpeg",width = 4.5, height =3,units = "in")



# annual, national by tech type ------------------------------------------------------------

with_shapes<-wide %>% filter(fuel=="Electric") %>% 
  left_join(mm_long, by=c("meas","end_use"="Scout_end_use","tech_stage","sector"))

with_shapes_agg <-with_shapes  %>% group_by(year,end_use,turnover,sector,description) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>% ungroup() 


print("printing 3")
#HVAC
for (s in c("com","res")){
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter((end_use %in% c("Cooling (Equip.)","Heating (Equip.)","Ventilation")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(end_use~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0("graphs/national_annual_",s,"_hvac.jpeg"),device = "jpeg",width = width, height =4,units = "in")
}


print("printing 4")
#water heating
for (s in c("com","res")){
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter((end_use %in% c("Water Heating")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0("graphs/national_annual_",s,"_wh.jpeg"),device = "jpeg",width = width, height =4/1.5,units = "in")
}

print("printing 5")
#non HVAC, non-WH
for (s in c("com","res")){
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter(!(end_use %in% c("Water Heating","Heating (Equip.)","Cooling (Equip.)","Ventilation")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0("graphs/national_annual_",s,"_non-mech.jpeg"),device = "jpeg",width = width, height =4/1.15,units = "in")
}



# annual by state and tech type for heating and cooling  ------------------------------------------------------------

print("printing 6")
with_shapes_agg_state <-with_shapes %>% filter(reg %in% states) %>% 
  group_by(year,reg,end_use,turnover,sector,description) %>% 
  summarize(TWh=sum(state_ann_kwh)/10^9) %>%
  ungroup()

for(s in c("res","com")){
  for (u in c("Heating (Equip.)","Cooling (Equip.)")){
    with_shapes_agg_state%>% 
      group_by(description) %>% filter(sum(TWh)>0) %>%
      filter(end_use ==u,sector==s) %>%
      ggplot(aes(x=year,y=TWh,fill=description)) +
      geom_area()+
      facet_grid(reg~turnover,labeller = labeller(turnover=to,end_use=eu),scales="free")+
      scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)))+ 
      scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
      scale_fill_manual(values=colors,name="")+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
    ggsave(paste0("graphs/state_annual_",s,"_",eu[u],".jpeg"),device = "jpeg",width = width, height =state_height,units = "in")
  }
}

