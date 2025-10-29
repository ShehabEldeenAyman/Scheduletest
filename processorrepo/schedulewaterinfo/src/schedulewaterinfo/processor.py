import logging
from dataclasses import dataclass
from logging import getLogger, Logger

from rdfc_runner import Processor, ProcessorArgs, Reader, Writer

from pywaterinfo import Waterinfo
import pandas as pd
import time
import os
import asyncio
import schedule


# --- Type Definitions ---
@dataclass
class TemplateArgs(ProcessorArgs):
    writer: Writer
    loc: str


# --- Processor Implementation ---
class schedulewaterinfo(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))
        ##############################
        self.vmm = Waterinfo("vmm", cache=True)
        self.hic = Waterinfo("hic", cache=True)
        self.base_run = False
        self.Gent_Terneuzen_River_Stage = None  
        if(self.base_run==False):
            self.Gent_Terneuzen_River_Stage = self.fetch_river_stage_2D()
            self.base_run = True
            self.logger.info("Initial fetching is done.")

    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        self.logger.debug("Initializing TemplateProcessor with args: {}", self.args)

    async def transform(self) -> None:
        pass

    async def produce(self) -> None:
        #schedule.every(1).minutes.do(self.custom_scheduler)
        schedule.every(1).minutes.do(lambda: asyncio.create_task(self.custom_scheduler()))
        while True:
            print("Scheduler started. Running every 1 minutes...")
            schedule.run_pending()
            #time.sleep(1)
            await asyncio.sleep(1)
        
        
    ###################################
    async def custom_scheduler(self):
        self.logger.info("Fetching river stage data...")
        Gent_Terneuzen_River_Stage_30_min = self.fetch_river_stage_30M()
        Gent_Terneuzen_River_Stage_combined = self.concat(self.Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min)
        self.debug_save(Gent_Terneuzen_River_Stage_combined)
        self.logger.info("Saved combined data successfully.")
        #await self.args.writer.string(Gent_Terneuzen_River_Stage_combined.to_csv())#convert to csv and not save it to disk.
        await self.args.writer.string("Help!!!")
        self.logger.info(f"CSV length: {len(Gent_Terneuzen_River_Stage_combined.to_csv())}")
        self.logger.info("You have reached the end of the loop")

    def fetch_river_stage_2D(self):
        #Baseline get data for the past 48 hours.
        Gent_Terneuzen_River_Stage = self.hic.get_timeseries_values("98536010",period='P2D')
        Gent_Terneuzen_River_Stage["Timestamp"] = Gent_Terneuzen_River_Stage["Timestamp"].dt.tz_localize(None)
        Gent_Terneuzen_River_Stage["Timestamp"] = Gent_Terneuzen_River_Stage["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return Gent_Terneuzen_River_Stage


    def fetch_river_stage_30M(self):
        #Get readings for the past 30 minutes
        Gent_Terneuzen_River_Stage_30_min = self.hic.get_timeseries_values("98536010",period='PT30M')
        Gent_Terneuzen_River_Stage_30_min["Timestamp"] = Gent_Terneuzen_River_Stage_30_min["Timestamp"].dt.tz_localize(None)
        Gent_Terneuzen_River_Stage_30_min["Timestamp"] = Gent_Terneuzen_River_Stage_30_min["Timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return Gent_Terneuzen_River_Stage_30_min

    def concat(self,Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min):
        Gent_Terneuzen_River_Stage_combined = pd.concat([Gent_Terneuzen_River_Stage,Gent_Terneuzen_River_Stage_30_min]).drop_duplicates(subset=["Timestamp"]).reset_index(drop=True)
        return Gent_Terneuzen_River_Stage_combined

    def debug_save(self,Gent_Terneuzen_River_Stage_combined):    
        Gent_Terneuzen_River_Stage_combined.to_csv(self.args.loc)    
        #Gent_Terneuzen_River_Stage_combined.to_csv('Gent_Terneuzen_River_Stage_combined.csv')

