import json
import os
import pandas as pd
import requests
from datetime import datetime


# # # # # # # # TO RUN THIS FILE LOCALLY UNCOMMENT BELOW # # # # # # # # #
# # See readme for more details.
# path = './'
# file_name = 'USNVC v2.02 export 2018-03'


# def send_final_result(obj):
#     print(json.dumps(obj))


# def send_to_stage(obj, stage):
#     globals()['process_{}'.format(stage)](path, file_name,
#                                           ch_ledger(), send_final_result,
#                                           send_to_stage, obj)


# class ch_ledger:
#     def log_change_event(self, name, description, source_data, changed_data):
#         print(name, description, source_data, changed_data)


# def main():
#     process_1(path, file_name, ch_ledger(),
#               send_final_result, send_to_stage, None)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# The first processing stage.
# It creates 1 final result and many other results that it sends to the next
#  stage for further processing.
# It returns count which is the number of rows created by this stage.
def process_1(path, file_name, ch_ledger, send_final_result,
              send_to_stage, previous_stage_result):
    preprocess_result = preprocess_usnvc(path)

    # Prep Database
    # I opted to rework the workflow to build the database iteratively as we
    # loop through source records. This codeblock wipes out the current
    # collection and starts fresh with a root document.
    nvcsUnits = preprocess_result['nvcsUnits']
    root = logical_nvcs_root(nvcsUnits)
    root['id'] = '0'
    send_final_result({'source_data': root, 'row_identifier': '0'})
    count = 1
    for index, row in nvcsUnits.iterrows():
        send_to_stage({'index': index, 'row': row.to_json()}, 2)
        count += 1
    return count


# The second processing stage used the previous_stage_result and sends
#  a singe document to be handled as a final result.
# It returns 1
def process_2(path, file_name, ch_ledger, send_final_result,
              send_to_stage, previous_stage_result):

    preprocess_result = preprocess_usnvc(path)
    process_result = process_usnvc(
        path, preprocess_result, previous_stage_result)
    final_result = {'source_data': process_result,
                    'row_identifier': str(process_result['id'])}
    send_final_result(final_result)
    return 1


# Do the preprocessing of sb data files
def preprocess_usnvc(path):

    response = {
        'unitXSimilarUnit': None,
        'nvcsDistribution': None,
        'usfsEcoregionDistribution1994': None,
        'usfsEcoregionDistribution2007': None,
        'unitPredecessors': None,
        'obsoleteUnits': None,
        'obsoleteParents': None,
        'unitReferences': None,
        'nvcsUnits': None
    }

    path = path + ''
    processFiles = {}
    for root, d_names, f_names in os.walk(path):
        for f in f_names:
            if f.endswith(".txt"):
                processFiles[f] = os.path.join(root, f)

    # Unit Attributes, Hierarchy, and Descriptions
    # The following code block merges the unit and unit description tables into one
    #  dataframe that serves as the core data for processing.
    units = pd.read_csv(processFiles["unit.txt"], sep='\t', encoding="ISO-8859-1", dtype={
                        "element_global_id": str, "parent_id": str, "classif_confidence_id": int})
    unitDescriptions = pd.read_csv(
        processFiles["unitDescription.txt"], sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})
    codes_classificationConfidence = pd.read_csv(
        processFiles["d_classif_confidence.txt"], sep='\t', encoding="ISO-8859-1", dtype={"D_CLASSIF_CONFIDENCE_ID": int})
    codes_classificationConfidence.rename(
        columns={'D_CLASSIF_CONFIDENCE_ID': 'classif_confidence_id'}, inplace=True)
    response['nvcsUnits'] = pd.merge(units, unitDescriptions,
                                     how='left', on='element_global_id')
    response['nvcsUnits'] = pd.merge(response['nvcsUnits'], codes_classificationConfidence,
                                     how='left', on='classif_confidence_id')
    del units
    del unitDescriptions
    del codes_classificationConfidence

    # Unit References
    # The following dataframes assemble the unit by unit references into a merged
    #  dataframe for later query and processing when building the unit documents.
    unitByReference = pd.read_csv(processFiles["UnitXReference.txt"], sep='\t',
                                  encoding="ISO-8859-1", dtype={"element_global_id": str, "reference_id": str})
    references = pd.read_csv(
        processFiles["reference.txt"], sep='\t', encoding="ISO-8859-1", dtype={"reference_id": str})
    response['unitReferences'] = pd.merge(left=unitByReference, right=references,
                                          left_on='reference_id', right_on='reference_id')
    del unitByReference
    del references

    # Unit Predecessors
    # The following codeblock retrieves the unit predecessors for processing.
    response['unitPredecessors'] = pd.read_csv(processFiles["unitPredecessor.txt"], sep='\t',
                                               encoding="ISO-8859-1", dtype={"element_global_id": str, "predecessor_id": str})

    # Obsolete records
    # The following codeblock retrieves the two tables that contain references to
    #  obsolete units or names. We may want to examine this in future versions to
    #  move from simply capturing notes about obsolescence to keeping track of what
    #  is actually changing. Alternatively, we can keep with a whole dataset
    #  versioning construct if that works better for the community, but as soon as
    #  we start minting individual DOIs for the units, making them citable, that
    #  changes the dynamic in how we manage the data moving forward.
    response['obsoleteUnits'] = pd.read_csv(processFiles["unitObsoleteName.txt"],
                                            sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})
    response['obsoleteParents'] = pd.read_csv(
        processFiles["unitObsoleteParent.txt"], sep='\t', encoding="ISO-8859-1", dtype={"element_global_id": str})

    # Unit Distribution - Nations and Subnations
    # The following codeblock assembles the four tables that make up all the code
    #  references for the unit by unit distribution at the national level and then
    #  in North American states and provinces. I played around with adding a little
    #  bit of value to the nations structure by looking up names and setting up
    #  objects that contain name, abbreviation, uncertainty (true/false), and an
    #  info API reference. But I also kept the original raw string/list of national
    #  abbreviations. That process would be a lot smarter if I did it here by pulling
    #  together a distinct list of all referenced nation codes/abbreviations and then
    #  building a lookup dataframe on those. I'll revisit at some point or if the
    #  code bogs down, but the REST API call is pretty quick.
    unitXSubnation = pd.read_csv(
        processFiles["UnitXSubnation.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_CurrentPresAbs = pd.read_csv(
        processFiles["d_curr_presence_absence.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_DistConfidence = pd.read_csv(
        processFiles["d_dist_confidence.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_Subnations = pd.read_csv(
        processFiles["d_subnation.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    response['nvcsDistribution'] = pd.merge(left=unitXSubnation, right=codes_CurrentPresAbs,
                                            left_on='d_curr_presence_absence_id', right_on='D_CURR_PRESENCE_ABSENCE_ID')
    response['nvcsDistribution'] = pd.merge(left=response['nvcsDistribution'], right=codes_DistConfidence,
                                            left_on='d_dist_confidence_id', right_on='D_DIST_CONFIDENCE_ID')
    response['nvcsDistribution'] = pd.merge(left=response['nvcsDistribution'], right=codes_Subnations,
                                            left_on='subnation_id', right_on='subnation_id')
    del unitXSubnation
    del codes_CurrentPresAbs
    del codes_DistConfidence
    del codes_Subnations

    # USFS Ecoregions
    # There is a coded list of USFS Ecoregion information in the unit descriptions,
    #  but this would have to be parsed and referenced out anyway and the base
    #  information seems to come through a "unitX..." set of tables. This codeblock
    #  sets those data up for processing.
    unitXUSFSEcoregion1994 = pd.read_csv(
        processFiles["UnitXEcoregionUsfs1994.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_USFSEcoregions1994 = pd.read_csv(
        processFiles["d_usfs_ecoregion1994.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    unitXUSFSEcoregion2007 = pd.read_csv(
        processFiles["UnitXEcoregionUsfs2007.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_USFSEcoregions2007 = pd.read_csv(
        processFiles["d_usfs_ecoregion2007.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    codes_OccurrenceStatus = pd.read_csv(
        processFiles["d_occurrence_status.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)
    response['usfsEcoregionDistribution1994'] = pd.merge(
        left=unitXUSFSEcoregion1994, right=codes_USFSEcoregions1994, left_on='usfs_ecoregion_id', right_on='USFS_ECOREGION_ID')
    response['usfsEcoregionDistribution1994'] = pd.merge(left=response['usfsEcoregionDistribution1994'], right=codes_OccurrenceStatus,
                                                         left_on='d_occurrence_status_id', right_on='D_OCCURRENCE_STATUS_ID')
    response['usfsEcoregionDistribution2007'] = pd.merge(
        left=unitXUSFSEcoregion2007, right=codes_USFSEcoregions2007, left_on='usfs_ecoregion_2007_id', right_on='usfs_ecoregion_2007_id')
    response['usfsEcoregionDistribution2007'] = pd.merge(left=response['usfsEcoregionDistribution2007'], right=codes_OccurrenceStatus,
                                                         left_on='d_occurrence_status_id', right_on='D_OCCURRENCE_STATUS_ID')
    del unitXUSFSEcoregion1994
    del codes_USFSEcoregions1994
    del unitXUSFSEcoregion2007
    del codes_USFSEcoregions2007
    del codes_OccurrenceStatus

    # Similar Units
    # The similar units table has references to units that are similar to another
    #  with specific notes recorded by the editors.
    response['unitXSimilarUnit'] = pd.read_csv(
        processFiles["UnitXSimilarUnit.txt"], sep='\t', encoding="ISO-8859-1", dtype=str)

    return response


def logical_nvcs_root(nvcsUnits):
    classLevel = nvcsUnits.loc[nvcsUnits["parent_id"].isnull(), [
        "element_global_id"]]
    nvcsRootDoc = {}
    nvcsRootDoc["title"] = "US National Vegetation Classification"
    nvcsRootDoc["parent"] = None
    nvcsRootDoc["ancestors"] = None
    nvcsRootDoc["children"] = list(
        map(int, classLevel["element_global_id"].tolist()))
    nvcsRootDoc["Hierarchy"] = {"unitsort": str(0)}

    return nvcsRootDoc


# Do the processing of sb data files. Comments below are from Sky's source code.
def process_usnvc(path, context, event):

    # Process Source and Build NVCS Docs
    # The following code block is the meat of this process. It takes quite a while to run as there
    # are a number of steps and conditional logic that need to play out. I used a couple of guiding
    # principals in laying out these documents.
    # Store the data according to the basic pattern established by the ESA Veg Panel in helping to
    # design the online USNVC Explorer app so that it can pretty much be navigated and understood
    # in its "native" form.
    # Assign more human-friendly attribute names to the things that we will display to people, but
    # retain a few of the "ugly names" for things that have special meaning in the data assembly
    # process.
    # I ended up using the element_global_id as the unique id value in the documents as it is unique
    # across the recordset and will be used to maintain record integrity over time.
    # I build and store the same unit by unit snapshot of the surrounding hierarchy (ancestors and
    # immediate children) similar to how the current application works. I also store parent ID but
    # build children and ancestors at the root level of the documents according to document database
    # best practices and for later processing.
    # For help in later presentation and usability of the structure, I create a logical root document
    # with an ID of 0 and a small amount of information. The "parentless" Class and Cultural Class
    # units are assigned this unit as parent.
    # Quite a bit of conditional logic goes into building display title from other attributes, and
    # I pull this up to the top of the document as "title" for convenience in later building out
    # the hierarchy.

    # Similar Units
    # The similar units table has references to units that are similar to another
    #  with specific notes recorded by the editors.

    unitXSimilarUnit = context['unitXSimilarUnit']
    nvcsDistribution = context['nvcsDistribution']
    usfsEcoregionDistribution1994 = context['usfsEcoregionDistribution1994']
    usfsEcoregionDistribution2007 = context['usfsEcoregionDistribution2007']
    unitPredecessors = context['unitPredecessors']
    obsoleteUnits = context['obsoleteUnits']
    obsoleteParents = context['obsoleteParents']
    unitReferences = context['unitReferences']
    nvcsUnits = context['nvcsUnits']

    data = event
    index = data['index']

    row = pd.Series(json.loads(data['row']))

    unitDoc = {"Identifiers": {}, "Overview": {}, "Hierarchy": {}, "Vegetation": {}, "Environment": {}, "Distribution": {}, "Plot Sampling and Analysis": {
    }, "Confidence Level": {}, "Conservation Status": {}, "Hierarchy": {}, "Concept History": {}, "Synonymy": {}, "Authorship": {}, "References": []}

    unitDoc["Date Processed"] = datetime.utcnow().isoformat()

    unitDoc["Identifiers"]["element_global_id"] = int(row["element_global_id"])
    unitDoc["Identifiers"]["Database Code"] = row["databasecode"]
    unitDoc["Identifiers"]["Classification Code"] = row["classificationcode"]

    unitDoc["Overview"]["Scientific Name"] = row["scientificname"]
    unitDoc["Overview"]["Formatted Scientific Name"] = clean_string(
        row["formattedscientificname"])
    unitDoc["Overview"]["Translated Name"] = row["translatedname"]
    if type(row["colloquialname"]) is str:
        unitDoc["Overview"]["Colloquial Name"] = row["colloquialname"]
    if type(row["typeconceptsentence"]) is str:
        unitDoc["Overview"]["Type Concept Sentence"] = clean_string(
            row["typeconceptsentence"])
    if type(row["typeconcept"]) is str:
        unitDoc["Overview"]["Type Concept"] = clean_string(row["typeconcept"])
    if type(row["diagnosticcharacteristics"]) is str:
        unitDoc["Overview"]["Diagnostic Characteristics"] = clean_string(
            row["diagnosticcharacteristics"])
    if type(row["rationale"]) is str:
        unitDoc["Overview"]["Rationale for Nonimal Species or Physiognomic Features"] = clean_string(
            row["rationale"])
    if type(row["classificationcomments"]) is str:
        unitDoc["Overview"]["Classification Comments"] = clean_string(
            row["classificationcomments"])
    if type(row["othercomments"]) is str:
        unitDoc["Overview"]["Other Comments"] = clean_string(
            row["othercomments"])

    if type(row["similarnvctypescomments"]) is str:
        unitDoc["Overview"]["Similar NVC Type Comments"] = clean_string(
            row["similarnvctypescomments"])
    thisSimilarUnits = unitXSimilarUnit.loc[unitXSimilarUnit["element_global_id"]
                                            == row["element_global_id"]]
    if len(thisSimilarUnits.index) > 0:
        unitDoc["Overview"]["Similar NVC Types"] = thisSimilarUnits.to_dict(
            "records")

    if row["hierarchylevel"] in ["Class", "Subclass", "Formation", "Division"]:
        unitDoc["Overview"]["Display Title"] = row["classificationcode"] + \
            " "+row["colloquialname"]+" "+row["hierarchylevel"]
    elif row["hierarchylevel"] in ["Macrogroup", "Group"]:
        unitDoc["Overview"]["Display Title"] = row["classificationcode"] + \
            " "+row["translatedname"]
    else:
        unitDoc["Overview"]["Display Title"] = row["databasecode"] + \
            " "+row["translatedname"]

    unitDoc["title"] = unitDoc["Overview"]["Display Title"]

    if type(row["physiognomy"]) is str:
        unitDoc["Vegetation"]["Physiognomy and Structure"] = clean_string(
            row["physiognomy"])
    if type(row["floristics"]) is str:
        unitDoc["Vegetation"]["Floristics"] = clean_string(row["floristics"])
    if type(row["dynamics"]) is str:
        unitDoc["Vegetation"]["Dynamics"] = clean_string(row["dynamics"])

    if type(row["environment"]) is str:
        unitDoc["Environment"]["Environmental Description"] = clean_string(
            row["environment"])

    if type(row["spatialpattern"]) is str:
        unitDoc["Environment"]["Spatial Pattern"] = clean_string(
            row["spatialpattern"])

    if type(row["range"]) is str:
        unitDoc["Distribution"]["Geographic Range"] = row["range"]

    if type(row["nations"]) is str:
        unitDoc["Distribution"]["Nations"] = {
            "Raw List": row["nations"], "Nation Info": []}
        for nation in row["nations"].split(","):
            thisNation = {"Abbreviation": nation.replace("?", "").strip()}
            if nation.endswith("?"):
                placeCodeUncertainty = True
            else:
                placeCodeUncertainty = False

            unitDoc["Distribution"]["Nations"]["Nation Info"].append(
                getPlaceCodeData(nation, placeCodeUncertainty))

    if type(row["subnations"]) is str:
        unitDoc["Distribution"]["Subnations"] = {"Raw List": row["subnations"]}

    thisDistribution = nvcsDistribution.loc[nvcsDistribution["element_global_id"]
                                            == row["element_global_id"]]
    if len(thisDistribution.index) > 0:
        unitDoc["Distribution"]["States/Provinces Raw Data"] = thisDistribution.to_dict(
            "records")

    thisUSFSDistribution1994 = usfsEcoregionDistribution1994.loc[
        usfsEcoregionDistribution1994["element_global_id"] == row["element_global_id"]]
    if len(thisUSFSDistribution1994.index) > 0:
        unitDoc["Distribution"]["1994 USFS Ecoregion Raw Data"] = thisUSFSDistribution1994.to_dict(
            "records")

    thisUSFSDistribution2007 = usfsEcoregionDistribution2007.loc[
        usfsEcoregionDistribution2007["element_global_id"] == row["element_global_id"]]
    if len(thisUSFSDistribution2007.index) > 0:
        unitDoc["Distribution"]["2007 USFS Ecoregion Raw Data"] = thisUSFSDistribution2007.to_dict(
            "records")

    if type(row["tncecoregions"]) is int:
        unitDoc["Distribution"]["TNC Ecoregions"] = row["tncecoregions"]

    if type(row["omernikecoregions"]) is int:
        unitDoc["Distribution"]["Omernik Ecoregions"] = row["omernikecoregions"]

    if type(row["omernikecoregions"]) is int:
        unitDoc["Distribution"]["Omernik Ecoregions"] = row["omernikecoregions"]

    if type(row["federallands"]) is int:
        unitDoc["Distribution"]["Federal Lands"] = row["federallands"]

    if type(row["plotcount"]) is int:
        unitDoc["Plot Sampling and Analysis"]["Plot Count"] = row["plotcount"]
    if type(row["plotsummary"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Summary"] = row["plotsummary"]
    if type(row["plottypal"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Type"] = row["plottypal"]
    if type(row["plotarchived"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Archive"] = row["plotarchived"]
    if type(row["plotconsistency"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Consistency"] = row["plotconsistency"]
    if type(row["plotsize"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Size"] = row["plotsize"]
    if type(row["plotmethods"]) is str:
        unitDoc["Plot Sampling and Analysis"]["Plot Methods"] = row["plotmethods"]

    unitDoc["Confidence Level"]["Confidence Level"] = row["CLASSIF_CONFIDENCE_DESC"]
    if type(row["confidencecomments"]) is str:
        unitDoc["Confidence Level"]["Confidence Level Comments"] = clean_string(
            row["confidencecomments"])

    if type(row["grank"]) is str:
        unitDoc["Conservation Status"]["Global Rank"] = row["grank"]
    if type(row["grankreviewdate"]) is str:
        unitDoc["Conservation Status"]["Global Rank Review Date"] = row["grankreviewdate"]
    if type(row["grankauthor"]) is str:
        unitDoc["Conservation Status"]["Global Rank Author"] = row["grankauthor"]
    if type(row["grankreasons"]) is str:
        unitDoc["Conservation Status"]["Global Rank Reasons"] = row["grankreasons"]

    unitDoc["Hierarchy"]["parent_id"] = str(row["parent_id"])
    unitDoc["Hierarchy"]["hierarchylevel"] = row["hierarchylevel"]
    unitDoc["Hierarchy"]["d_classification_level_id"] = row["d_classification_level_id"]
    unitDoc["Hierarchy"]["unitsort"] = row["unitsort"]
    unitDoc["Hierarchy"]["parentkey"] = row["parentkey"]
    unitDoc["Hierarchy"]["parentname"] = row["parentname"]

    try:
        unitDoc["parent"] = int(row["parent_id"])
    except:
        unitDoc["parent"] = int(0)

    thisHierarchyData = get_hierarchy_from_df(
        row["element_global_id"], nvcsUnits)
    unitDoc["children"] = thisHierarchyData["Children"]
    unitDoc["Hierarchy"]["Cached Hierarchy"] = thisHierarchyData["Hierarchy"]
    if len(thisHierarchyData["Ancestors"]) > 0:
        unitDoc["ancestors"] = thisHierarchyData["Ancestors"]
    else:
        unitDoc["ancestors"] = [int(0)]

    if type(row["lineage"]) is str:
        unitDoc["Concept History"]["Concept Lineage"] = row["lineage"]

    thisUnitPredecessors = unitPredecessors.loc[unitPredecessors["element_global_id"]
                                                == row["element_global_id"]]
    if len(thisUnitPredecessors.index) > 0:
        unitDoc["Concept History"]["Predecessors Raw Data"] = thisUnitPredecessors.to_dict(
            "records")

    thisUnitObsoleteUnits = obsoleteUnits.loc[obsoleteUnits["element_global_id"]
                                              == row["element_global_id"]]
    if len(thisUnitObsoleteUnits.index) > 0:
        unitDoc["Concept History"]["Obsolete Units Raw Data"] = thisUnitObsoleteUnits.to_dict(
            "records")

    thisUnitObsoleteParents = obsoleteParents.loc[obsoleteParents["element_global_id"]
                                                  == row["element_global_id"]]
    if len(thisUnitObsoleteParents.index) > 0:
        unitDoc["Concept History"]["Obsolete Parents Raw Data"] = thisUnitObsoleteParents.to_dict(
            "records")

    if type(row["synonymy"]) is str:
        unitDoc["Synonymy"]["Synonymy"] = row["synonymy"]

    if type(row["primaryconceptsource"]) is str:
        unitDoc["Authorship"]["Concept Author"] = row["primaryconceptsource"]
    if type(row["descriptionauthor"]) is str:
        unitDoc["Authorship"]["Description Author"] = row["descriptionauthor"]
    if type(row["acknowledgements"]) is str:
        unitDoc["Authorship"]["Acknowledgements"] = row["acknowledgements"]
    if type(row["versiondate"]) is str:
        unitDoc["Authorship"]["Version Date"] = row["versiondate"]

    thisUnitReferences = unitReferences.loc[unitReferences["element_global_id"]
                                            == row["element_global_id"]]
    for index, row in thisUnitReferences.iterrows():
        unitDoc["References"].append(
            {"Short Citation": row["shortcitation"], "Full Citation": row["fullcitation"]})
    unitDoc['id'] = str(row["element_global_id"])
    return unitDoc


def clean_string(text):
    replacements = {'&amp;': '&', '&lt;': '<', '&gt;': '>'}
    for x, y in replacements.items():
        text = text.replace(x, y)
    return (text)


def get_hierarchy_from_df(element_global_id, nvcsUnits):
    # Assumes the full dataframe exists in memory here already
    thisUnitData = nvcsUnits.loc[nvcsUnits["element_global_id"] == str(element_global_id), [
        "element_global_id", "parent_id", "hierarchylevel", "classificationcode", "databasecode", "translatedname", "colloquialname", "unitsort", "DISPLAY_ORDER"]]

    immediateChildren = nvcsUnits.loc[nvcsUnits["parent_id"] == str(element_global_id), [
        "element_global_id", "parent_id", "hierarchylevel", "classificationcode", "databasecode", "translatedname", "colloquialname", "unitsort", "DISPLAY_ORDER"]]

    parentID = thisUnitData["parent_id"].values[0]

    ancestors = []
    while type(parentID) is str:
        ancestor = nvcsUnits.loc[nvcsUnits["element_global_id"] == str(parentID), [
            "element_global_id", "parent_id", "hierarchylevel", "classificationcode", "databasecode", "translatedname", "colloquialname", "unitsort", "DISPLAY_ORDER"]]
        ancestors = ancestors + ancestor.to_dict("records")
        parentID = ancestor["parent_id"].values[0]

    hierarchyList = []
    for record in ancestors+thisUnitData.to_dict("records")+immediateChildren.to_dict("records"):
        if record["hierarchylevel"] in ["Class", "Subclass", "Formation", "Division"]:
            record["Display Title"] = record["classificationcode"] + \
                " "+record["colloquialname"]+" "+record["hierarchylevel"]
        elif record["hierarchylevel"] in ["Macrogroup", "Group"]:
            record["Display Title"] = record["classificationcode"] + \
                " "+record["translatedname"]
        else:
            record["Display Title"] = record["databasecode"] + \
                " "+record["translatedname"]
        hierarchyList.append(record)

    return {"Children": list(map(int, immediateChildren["element_global_id"].tolist())), "Hierarchy": hierarchyList, "Ancestors": list(map(int, [a["element_global_id"] for a in ancestors]))}


knownPlaceCodes = {}


def getPlaceCodeData(abbreviation, uncertainty=False):

    codeData = {}
    codeData["Abbreviation"] = abbreviation
    codeData["Uncertainty"] = uncertainty
    codeData["Info API"] = "https://restcountries.eu/rest/v2/alpha/"+abbreviation

    if abbreviation in knownPlaceCodes.keys():
        codeData["Name"] = knownPlaceCodes[abbreviation]
    else:
        thisNationInfo = requests.get(
            codeData["Info API"]+"?fields=name").json()
        if "name" in thisNationInfo.keys():
            codeData["Name"] = thisNationInfo["name"]
        else:
            codeData["Name"] = "Unknown"

    return codeData


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        pass
