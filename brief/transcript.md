Meeting Title: Iron Ore Plant Monitoring Solution

Transcript:

You: Let's start. So we're going to start with a synthetic data generator that's going to generate synthetic tag data and scatter data from an iron ore crushing processing plant.

You: It needs to include tag data to represent crushes screens a descents circuit and stockpile stacking as well, including a stockpile like stacking indicator.

You: We're going to need vibration sensors as well. And the idea is this tag data should be useful for mass balance calculations and to build a mass balance flow sheet visually over time and to build out a predictive maintenance model using the vibration sensors.

You: Should we just give a bit of an overview of like the whole thing? Like what's the... Yeah, sure, sure. So that data set is going to go into... a Databricks Spark declarative pipeline running in real-time mode.

You: It's going to write to a Lake-based sync. That pipeline will also have some logic to look at simple rules, and if any row fails

You: that rule, it will actually write to an API sync, which will trigger an alarm. The Lake-based data written from that pipeline will then use a change data feed and write

You: to a Delta table in Databricks. We will then chain a Spark declarative pipeline off that Delta table to then break that raw

You: data set out into analytical versions of Delta tables, which are more contextualized for human consumption. We're then going to train a machine learning model for predictive maintenance off of the

You: vibration sensor data at that level, along with Delta tables. off of the lake base data to actually do a mass balance flow sheet showing real

You: time mass balance as the tag data flows through and then we're going to wrap AI on top of the analytical Delta tables that's going to be able to call the ML

You: model to run real-time predictive maintenance or look at batch scores they're generated and then we're going to be able to ask you know what's happening in my plant in general and and then so but this is like the scenarios

You: this is a company that processes iron ore it's an iron ore iron ore mine and was looking specifically at a iron ore crushing plant and we want to build

You: effectively a real-time mass balance monitoring solution that can visually display it control room style and also use AI and mm-hmm all done in Databricks

You: all done in Databricks please