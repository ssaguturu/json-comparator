package com.mams.comparator.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.deser.std.UUIDDeserializer;
import com.fasterxml.jackson.databind.ser.std.UUIDSerializer;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static com.mams.comparator.json.CompareJsons.DiffModes.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class CompareJsons {

    private static ObjectMapper objectMapper;

    private static PropertyResourceBundle configs;

    private static String env1URL, env2URL;

    private static Map<String, List<String>> elementsToCompare = new HashMap<>();

    private static PropertyResourceBundle getPropertiesBundle(String config) {
        try {
            return new PropertyResourceBundle(new InputStreamReader(FileReader.class.getResourceAsStream(config)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void initialize(String[] args) {
        configs = getPropertiesBundle("/config.properties");
        objectMapper = getObjectMapper();
        Enumeration<String> keys = configs.getKeys();
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            if(key.endsWith("compare.elements.names")) {
                elementsToCompare.put(key, Arrays.asList(configs.getString("compare.elements.names").split(",")));
            }
        }
        env1URL = configs.getString(args[0]+".endpoint.url");
        env2URL = configs.getString(args[1]+".endpoint.url");
    }

    static void testPrint(String[] args) {
        printRow("Property", args[1], args[2], "Operation", true);
        printRow("name",
                                    "111111111111111111111111111111111111111111111111111111111111111111111111",
                                    "222222222222222222222222222222222222222222222222222222222222222222222222", "MODIFIED", false);
        printRow("=================================================",
                "{\"operator\":\">\",\"aggregator\":\"LATEST\",\"dataType\":\"/biometric/BloodSugarNumberActualmgdL\",\"targetValue\":\"0\"}",
                "4444", "MODIFIED", false);
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {

        args = getArgumentsIfRequired(args);
        initialize(args);

        String env1 = args[0];
        String env2 = args[1];

        List<List<String>> programListSource = getPrograms(env1URL);
        List<List<String>> programListTarget = getPrograms(env2URL);

        programListSource.stream().forEach(srcProgramDetails -> {
            Optional<List<String>> matchedProgram = programListTarget.stream().filter(trgProgramDetails -> {
                return srcProgramDetails.get(1).equalsIgnoreCase(trgProgramDetails.get(1));
            }).findFirst();
            if(matchedProgram.isPresent()) {
                try {
                    System.out.println("\n\n\n");
                    System.out.println("~~~~~~~~~~~~~ Processing Program: " + srcProgramDetails.get(1) + " ~~~~~~~~~~~~~");
                    System.out.println();
                    compareAndPrintDiff(
                            env1, fetchJson(env1URL+"/"+srcProgramDetails.get(0)),
                            env2, fetchJson(env2URL+"/"+matchedProgram.get().get(0))
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

//        String jsonOne = fetchProgramConfiguration(args[0], args[1]);
//        String jsonTwo = fetchProgramConfiguration(args[0], args[2]);
//
//        compareAndPrintDiff(args[0], jsonOne, args[1], jsonTwo);

    }

    private static void compareAndPrintDiff(String env1, String jsonOne, String env2, String jsonTwo) throws IOException {

        JsonNode srcJsonNode  = objectMapper.readTree(jsonOne);
        JsonNode destJsonNode = objectMapper.readTree(jsonTwo);
        Map<String, List<String>> diffs = compareObject(srcJsonNode, destJsonNode, "", "");
        displayDiffs(diffs, env1, env2);
    }

    private static Map<String, List<String>> compareObject(JsonNode srcJsonNode, JsonNode destJsonNode, String path, String reportKey) {

        if(srcJsonNode.equals(destJsonNode)) {
            return Collections.emptyMap();
        }

        Map<String, List<String>> rootProperties = new LinkedHashMap<>();
        Map<String, List> arrayProperties = new LinkedHashMap<>();

        srcJsonNode.fieldNames().forEachRemaining(fieldName -> {
            JsonNode srcProp = srcJsonNode.get(fieldName);
            JsonNode destProp = destJsonNode.get(fieldName);
            String fieldPath = isEmpty(path) ? fieldName :  path + "." + fieldName;
            String localReportKey = isEmpty(reportKey) ? fieldName :  reportKey + "." + fieldName;
            if (isPropertyNeededForComparision(path, fieldName)) {
                if (!srcProp.equals(destProp)) {
                    if (isArray(srcProp, destProp)) {
                        arrayProperties.put(fieldPath, asList(srcProp, destProp, localReportKey));
                    } else if(isNestedStructure(srcProp) || isNestedStructure(destProp)) {
                        rootProperties.putAll(compareObject(srcProp, destProp, fieldPath, localReportKey));
                    } else {
                        addDiff(localReportKey, getNodeNameSafe(srcProp), getNodeNameSafe(destProp), rootProperties);
                    }
                }
            }
        });

        arrayProperties.entrySet().stream().forEach(
                entry -> {
                    Map<? extends String, ? extends List<String>> diffs = compareArrays(
                            (JsonNode)entry.getValue().get(0),
                            (JsonNode) entry.getValue().get(1),
                            entry.getKey(),
                            (String) entry.getValue().get(2));
                    if(nonNull(diffs)) {
                        rootProperties.putAll(diffs);
                    }
                }
        );
        return rootProperties;
    }

    private static Map<? extends String,? extends List<String>> compareArrays(JsonNode srcJsonNode, JsonNode destJsonNode, String arrayName, String reportKey) {
        String matchingPropertyName = getMatchingPropertyName(arrayName);
        if(isEmpty(matchingPropertyName)) {
            return Collections.emptyMap();
        }

        List<JsonNode> srcNodesList  = new ArrayList<>();
        List<JsonNode> destNodesList = new ArrayList<>();
        if(nonNull(srcJsonNode))
            srcJsonNode .iterator().forEachRemaining(node -> srcNodesList.add(node));

        if(nonNull(destJsonNode))
            destJsonNode.iterator().forEachRemaining(node -> destNodesList.add(node));

        Map<String, List<String>> diffs = new LinkedHashMap<>();

        srcNodesList.forEach(srcNode -> {
            Optional<JsonNode> opt;
            String identityFieldName=matchingPropertyName;
            JsonNode identityFieldValue = srcNode.get(matchingPropertyName);
            if("OBJECT".equals(matchingPropertyName)) {
                opt = destNodesList.stream().filter(destNode -> srcNode.equals(destNode)).findFirst();
                identityFieldValue=srcNode.get(identityFieldName=getIdentifiableFieldColumn(srcNode));
            } else {
                opt = destNodesList.stream().filter(destNode -> srcNode.get(matchingPropertyName).equals(destNode.get(matchingPropertyName))).findFirst();
            }

            if(opt.isPresent()) {
                Map<String, List<String>> map = compareObject(srcNode, opt.get(), arrayName, getReportKey(reportKey, identityFieldName, identityFieldValue));
                diffs.putAll(map);
                destNodesList.remove(opt.get());
            } else {
                addDiff(getReportKey(reportKey, identityFieldName, identityFieldValue), String.valueOf(srcNode), null, diffs);
            }
        });
        destNodesList.forEach(destNode -> {
            addDiff(generateKeyForArrayItem(arrayName, destNode), null, String.valueOf(destNode), diffs);
        });

        return diffs;
    }

    private static String getReportKey(String reportKey, String identityFieldName, JsonNode identityFieldValue) {
        return new StringBuilder().append(reportKey).append("[").append(identityFieldName).append("=").append(String.valueOf(identityFieldValue)).append("]").toString();
    }

    private static void addDiff(String fieldPath, String srcValue, String destValue, Map<String, List<String>> map) {
        srcValue  = isNull(srcValue)  || isEmpty(srcValue)  || "null".equalsIgnoreCase(srcValue)  ? null : srcValue.trim();
        destValue = isNull(destValue) || isEmpty(destValue) || "null".equalsIgnoreCase(destValue) ? null : destValue.trim();

        if (isNull(srcValue) && isNull(destValue)) {
            return;
        }
        DiffModes mode = MODIFIED;
        if(isNull(srcValue) && nonNull(destValue)) {
            mode = ADDED;
        } else if(nonNull(srcValue) && isNull(destValue)) {
            mode = DELETED;
        }
        map.put(fieldPath, asList(srcValue, destValue, mode.toString()));
    }

    private static boolean isArray(JsonNode srcProp, JsonNode destProp) {
        return (nonNull(srcProp) && srcProp.isArray()) || (nonNull(destProp) && destProp.isArray());
    }

    private static String getIdentifiableFieldColumn(JsonNode node) {
        StringBuilder idField=new StringBuilder();
        Iterator<String> fieldNames = node.fieldNames();
        fieldNames.forEachRemaining(fieldName -> {
            if(idField.length() == 0) {
                idField.append(fieldName);
            }
            if("id".equalsIgnoreCase(fieldName) || "name".equalsIgnoreCase(fieldName)) {
                idField.setLength(0);
                idField.append(fieldName);
            }
        });
        return idField.toString();
    }

    private static String generateKeyForArrayItem(String key, JsonNode node) {
        String tempKey = key;

        String identityFieldName=getIdentifiableFieldColumn(node);

        boolean nestedStructure = isNestedStructure(node);
        String arrAttribute = "[" + identityFieldName + "=" + node.get(identityFieldName).asText() + "]";
        if(tempKey.contains(".")) {
            int index = tempKey.lastIndexOf('.');
            if(nestedStructure) {
                if (tempKey.substring(0, index).endsWith("]")) {
                    tempKey = tempKey + arrAttribute;
                } else {
                    tempKey = tempKey + arrAttribute;
                }
            } else {
                tempKey = tempKey.substring(0, index) + arrAttribute + tempKey.substring(index);
            }
        } else {
            tempKey=tempKey + arrAttribute;
        }
        return tempKey;
    }

    private static boolean isNestedStructure(JsonNode srcNode) {
        return nonNull(srcNode) && (srcNode.isArray() || srcNode.size() != 0 );
    }

    private static String getMatchingPropertyName(String arrayName) {
        return configs.getString(arrayName+".matching.property");
    }

    private static String getNodeNameSafe(JsonNode jsonNode) {
        return isNull(jsonNode) ? null : jsonNode.asText();
    }

    private static boolean isPropertyNeededForComparision(String basePath, String fieldName) {
        String propName = isEmpty(basePath) ? "" : basePath + ".";
        String props = configs.getString( propName + "compare.elements.names");
        if(isEmpty(props)) {
            return false;
        }

        if("ALL".equalsIgnoreCase(props)) {
            return true;
        }
        return asList(props.split(",")).contains(fieldName);
    }

    private static String[] getArgumentsIfRequired(String[] args) {
        if(isNull(args) || args.length < 2) {
            args = new String[3];
            args[0] = "qa";
            args[1] = "qaa";
            args[2] = "know_your_numbers.json";
        }
        return args;
    }

    private static String fetchProgramConfiguration(String programName, String env) throws IOException {
        return getFileContentsAsString(String.join("/","/program-configurations", env, programName));
    }

    public static String getFileContentsAsString(String path) throws IOException {
        StringBuffer buf = new StringBuffer();
        BufferedReader txtReader = new BufferedReader(new InputStreamReader(FileReader.class.getResourceAsStream(path)));
        for (String line; (line = txtReader.readLine()) != null; ) {
            buf.append(line + "\n");
        }
        System.out.println();
        return buf.toString();
    }

    public static ObjectMapper getObjectMapper() {
        if(isNull(objectMapper)) {
            Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();

            builder.serializers(new UUIDSerializer());
            builder.deserializers(new UUIDDeserializer());

            builder.propertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE);
            return builder.build();
        } else {
            return objectMapper;
        }
    }

    private static void displayDiffs(Map<String, List<String>> diffs, String env1, String env2) {
        printRow("Property", env1, env2, "Diff", true);

        diffs.entrySet().forEach(entry -> printRow(entry.getKey(), entry.getValue().get(0), entry.getValue().get(1), entry.getValue().get(2), false));

    }

    private static void printRow(String property, String columnOneText, String columnTwoText, String operation, boolean isHeader) {
        property = isNull(property) ? "" : property;
        columnOneText = isNull(columnOneText) ? "" : columnOneText;
        columnTwoText = isNull(columnTwoText) ? "" : columnTwoText;
        operation = isNull(operation) ? "" : operation;

        int totalWidth = 150;
        int operationColumnWidth = 16;
        if(isHeader) {
            System.out.println(getFormattedString("*", totalWidth + operationColumnWidth, '*', false, "", "", 1, false).get(0));
        }
        int noOfRows = 1;
        int tempNoOfRows = getNoOfRowsRequired(totalWidth/3 - 4, property.length());
        if(tempNoOfRows > noOfRows) {
            noOfRows = tempNoOfRows;
        }

        tempNoOfRows = getNoOfRowsRequired(totalWidth/3 - 3, columnOneText.length());
        if(tempNoOfRows > noOfRows) {
            noOfRows = tempNoOfRows;
        }

        tempNoOfRows = getNoOfRowsRequired(totalWidth/3 - 3, columnTwoText.length());
        if(tempNoOfRows > noOfRows) {
            noOfRows = tempNoOfRows;
        }

        List<String> propertyColumnList     = getFormattedString(property, totalWidth / 3 - 2,       ' ', true, "|", "|", noOfRows, isHeader ? true : false);
        List<String> columnOneList          = getFormattedString(columnOneText, totalWidth / 3 - 1,  ' ', true, "",  "|", noOfRows, isHeader ? true : false);
        List<String> columnTwoList          = getFormattedString(columnTwoText,  totalWidth / 3 - 1, ' ', true, "",  "|", noOfRows, isHeader ? true : false);
        List<String> operationColumnList    = getFormattedString(operation, operationColumnWidth - 1,' ', true, "", "|", noOfRows, true);
        for (int i =0; i < noOfRows; i++) {
            System.out.print(propertyColumnList.get(i));
            System.out.print(columnOneList.get(i));
            System.out.print(columnTwoList.get(i));
            System.out.println(operationColumnList.get(i));
        }
        if(isHeader) {
            System.out.println(getFormattedString("*", totalWidth + operationColumnWidth, '*', false, "", "", 1, false).get(0));
        } else {
            System.out.println(getFormattedString("-", totalWidth + operationColumnWidth, '-', false, "", "", 1, false).get(0));
        }
    }

    private static List<String> getFormattedString(String srcText, int size, char defaultCharToBeFilled, boolean needSpace, String prefix, String suffix, int noOfRows, boolean isCenterAligned) {
        List<String> tokens = new ArrayList<>();
        StringBuilder formattedText = new StringBuilder(size);

        String currentLineText = srcText;

        srcText = isNull(srcText) ? "" : srcText;
        int textLength = srcText.length();
        if (textLength < size) {
            boolean flag = true;
            while (--noOfRows >= 0) {
                formattedText.append(prefix);
                if(flag) {

                    int leftPaddingSize = (size - textLength) / 2;
                    int rightPaddingSize = size - leftPaddingSize - textLength;
                    if (needSpace) {
                        leftPaddingSize--;
                        rightPaddingSize--;
                        currentLineText = defaultCharToBeFilled + srcText + defaultCharToBeFilled;
                    }
                    if(!isCenterAligned) {
                        rightPaddingSize += leftPaddingSize;
                        leftPaddingSize = 0;
                    }
                    for (int i = 0; i < leftPaddingSize; i++) {
                        formattedText.append(defaultCharToBeFilled);
                    }
                    formattedText.append(isNull(srcText) ? "" : currentLineText);
                    for (int i = 0; i < rightPaddingSize; i++) {
                        formattedText.append(defaultCharToBeFilled);
                    }
                    flag = !flag;
                } else {
                    formattedText.append(format("%"+(size)+"s", defaultCharToBeFilled));
                }
                tokens.add(formattedText.append(suffix).toString());
                formattedText.setLength(0);
            }
        } else {
            while (noOfRows >= 0) {
                if (textLength > 0) {
                    while (textLength > 0) {
                        if (textLength > size - 3) {
                            currentLineText = srcText.substring(0, size - 3) + "-";
                            srcText = srcText.substring(size - 3);
                        } else {
                            currentLineText = format("%-" + (size - 2) + "s", srcText);
                            srcText = "";
                        }
                        formattedText.append(prefix).append(defaultCharToBeFilled).append(currentLineText).append(defaultCharToBeFilled).append(suffix);
                        textLength = srcText.length();
                        tokens.add(formattedText.toString());
                        formattedText.setLength(0);
                        noOfRows--;
                    }
                } else {
                    tokens.add(format("%s%"+size+"s%s", prefix, defaultCharToBeFilled, suffix));
                    noOfRows--;
                }
            }
        }
        return tokens;
    }

    private static int getNoOfRowsRequired(int columnWidth, int textWidth) {
        int noOfRows = 1;
        int mod = 0;
        if((mod = textWidth % columnWidth) > 0) {
            int tempNoOfRows = textWidth / columnWidth + (mod > 0 ? 1 : 0);
            if(tempNoOfRows > noOfRows) {
                noOfRows = tempNoOfRows;
            }
        }
        return noOfRows;
    }

    private static List<List<String>> getPrograms(String url) throws IOException {
        List<List<String>> programList = new ArrayList<List<String>>();
        RequestSpecification build = new RequestSpecBuilder()
                .setRelaxedHTTPSValidation().setContentType(ContentType.JSON).setBaseUri(url)
                .build();

        String jsonText = RestAssured.given().spec(build).get().asString();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(jsonText);

        for (int i=0; jsonNode.has(i); i++){
//            System.out.println( " Program Id is "+ jsonNode.get(i).get("id")); ;
//            System.out.println( " Program Id is "+ jsonNode.get(i).get("name")); ;
            ArrayList<String> programNode = new ArrayList<String>();
            programNode.add(jsonNode.get(i).get("id").asText());
            programNode.add(jsonNode.get(i).get("name").asText());
//            if("Personal Health Clinician".equalsIgnoreCase(programNode.get(1))) {
                programList.add(programNode);
//            }
        }
        return programList;
    }

    private static String fetchJson(String url) {
        RequestSpecification build = new RequestSpecBuilder()
                .setRelaxedHTTPSValidation()
                .setContentType(ContentType.JSON)
                .setBaseUri(url)
                .build();

        return( RestAssured.given().spec(build).get().asString()) ;
    }

    enum DiffModes {
        ADDED,
        MODIFIED,
        DELETED
    }
}

