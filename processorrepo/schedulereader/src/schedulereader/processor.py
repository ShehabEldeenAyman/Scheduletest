import logging
from dataclasses import dataclass
from logging import getLogger, Logger

from rdfc_runner import Processor, Reader, Writer

import pandas as pd
import time
import os
import asyncio
import schedule

# --- Type Definitions ---
@dataclass
class TemplateArgs:
    writer: Writer


# --- Processor Implementation ---
class schedulereader(Processor[TemplateArgs]):
    logger: Logger = getLogger('rdfc.TemplateProcessor')

    def __init__(self, args: TemplateArgs):
        super().__init__(args)
        self.logger.debug(msg="Created TemplateProcessor with args: {}".format(args))

    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        self.logger.debug("Initializing TemplateProcessor with args: {}".format(self.args))

    async def transform(self) -> None:
        pass


    async def produce(self) -> None:
        # #schedule.every(1).minutes.do(self.custom_scheduler)
        # schedule.every(1).minutes.do(lambda: asyncio.create_task(self.custom_reader()))
        # while True:
        #     print("Scheduler started. Running every 1 minutes...")
        #     schedule.run_pending()
        #     #time.sleep(1)
        #     await asyncio.sleep(1)
        while True:
            self.logger.info("Running scheduled reader...")
            await asyncio.sleep(90) 
            try:
                await self.custom_reader()
            except Exception as e:
                self.logger.error(f"Error in custom_reader: {e}")
    ###################################
    async def custom_reader(self):
        self.logger.info("Started reading data...")
        #file = pd.read_csv("./generated/Gent_Terneuzen_River_Stage_combined.csv")
        with open("./generated/Gent_Terneuzen_River_Stage_combined.csv", "r", encoding="utf-8") as f:
            content = f.read()
            #self.logger.info(content)
        await self.args.writer.string(content)


