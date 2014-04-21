package com.github.fakemongo.impl;

import com.github.fakemongo.FongoException;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateEngine {
  final static Logger LOG = LoggerFactory.getLogger(UpdateEngine.class);

  private final ExpressionParser expressionParser = new ExpressionParser();


  void keyCheck(String key, Set<String> seenKeys) {
    if (!seenKeys.add(key)) {
      throw new FongoException("attempting more than one atomic update on on " + key);
    }
  }


  abstract class BasicUpdate {

    private final boolean createMissing;
    final String command;

    public BasicUpdate(String command, boolean createMissing) {
      this.command = command;
      this.createMissing = createMissing;
    }

    abstract void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal);

    public DBObject doUpdate(DBObject obj, DBObject update, Set<String> seenKeys, DBObject query) {
      DBObject updateObject = (DBObject) update.get(command);
      HashSet<String> keySet = new HashSet<String>(updateObject.keySet());

      LOG.debug("KeySet is of length {}", keySet.size());

      for (String updateKey : keySet) {
        LOG.debug("\tfound a key {}", updateKey);

        keyCheck(updateKey, seenKeys);
        doSingleKeyUpdate(updateKey, obj, updateObject.get(updateKey), query);
      }
      return obj;
    }

    void doSingleKeyUpdate(final String updateKey, final DBObject objOriginal, Object object, DBObject query) {
      List<String> path = Util.split(updateKey);
      String subKey = path.get(0);
      DBObject obj = objOriginal;
      boolean isPositional = updateKey.contains(".$");
      if (isPositional) {
        LOG.debug("got a positional for query {}", query);
      }
      for (int i = 0; i < path.size() - 1; i++) {
        if (!obj.containsField(subKey)) {
          if (createMissing && !isPositional) {
            obj.put(subKey, new BasicDBObject());
          } else {
            return;
          }
        }
        Object value = obj.get(subKey);
        if ((value instanceof List) && "$".equals(path.get(i + 1))) {
          handlePositionalUpdate(updateKey, object, (List) value, obj, query, objOriginal);
        } else if (value instanceof DBObject) {
          obj = (DBObject) value;
        } else if (value instanceof List) {
          BasicDBList newList = Util.wrap((List) value);

          obj = newList;
        } else {
          throw new FongoException("subfield must be object. " + updateKey + " not in " + objOriginal);
        }
        subKey = path.get(i + 1);
      }
      if (!isPositional) {

        LOG.debug("Subobject is {}", obj);
        mergeAction(subKey, obj, object, objOriginal);
        LOG.debug("Full object is {}", objOriginal);

      }
    }

    public void handlePositionalUpdate(final String updateKey, Object object, List valueList, DBObject ownerObj, DBObject query, DBObject objOriginal) {
      int dollarIndex = updateKey.indexOf("$");
      String postPath = (dollarIndex == updateKey.length() - 1) ? "" : updateKey.substring(dollarIndex + 2);
      String prePath = updateKey.substring(0, dollarIndex - 1);
      //create a filter from the original query
      Filter filter = null;
      for (String key : query.keySet()) {
        if (key.startsWith(prePath)) {
          String matchKey = prePath.equals(key) ? key : key.substring(prePath.length() + 1);
          filter = expressionParser.buildFilter(new BasicDBObject(matchKey, query.get(key)));
        }
      }
      if (filter == null) {
        throw new FongoException("positional operator " + updateKey + " must be used on query key " + query);
      }

      // find the right item
      for (int i = 0; i < valueList.size(); i++) {
        Object listItem = valueList.get(i);
        if (LOG.isDebugEnabled()) {
          LOG.debug("found a positional list item " + listItem + " " + prePath + " " + postPath);
        }
        if (!postPath.isEmpty()) {
          if (!(listItem instanceof DBObject)) {
            throw new FongoException("can not update \"" + postPath + "\" field of non-DBObject object");
          }

          if (filter.apply((DBObject) listItem)) {
            doSingleKeyUpdate(postPath, (DBObject) listItem, object, query);
            break;
          }
        } else {
          //this is kind of a waste
          DBObject o = listItem instanceof DBObject ? (DBObject) listItem : new BasicDBObject(prePath, listItem);
          if (filter.apply(o)) {
            BasicDBList newList = new BasicDBList();
            newList.addAll(valueList);
            ownerObj.put(prePath, newList);
            mergeAction(String.valueOf(i), newList, object, objOriginal);
            break;
          }
        }
      }
    }
  }

  Number genericAdd(Number left, Number right) {
    if (left instanceof Float || left instanceof Double || right instanceof Float || right instanceof Double) {
      return left.doubleValue() + (right.doubleValue());
    } else if (left instanceof Integer) {
      return left.intValue() + (right.intValue());
    } else {
      return left.longValue() + (right.intValue());
    }
  }

  BasicDBList asDbList(Object... objects) {
    BasicDBList dbList = new BasicDBList();
    for (Object o : objects) {
      dbList.add(o);
    }
    return dbList;
  }

  final List<BasicUpdate> commands = Arrays.<BasicUpdate>asList(
      new BasicUpdate("$set", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          subObject.put(subKey, object);
        }
      },
      new BasicUpdate("$inc", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          Number updateNumber = expressionParser.typecast(command + " value", object, Number.class);
          Object oldValue = subObject.get(subKey);
          if (oldValue == null) {
            subObject.put(subKey, updateNumber);
          } else {
            Number oldNumber = expressionParser.typecast(subKey + " value", oldValue, Number.class);
            subObject.put(subKey, genericAdd(oldNumber, updateNumber));
          }
        }
      },
      new BasicUpdate("$unset", false) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          subObject.removeField(subKey);
        }
      },
      new BasicUpdate("$rename", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          Object objValue = subObject.removeField(subKey);
          String newKey = (String) object;
          Util.putValue(objOriginal, newKey, objValue);
        }
      },
      new BasicUpdate("$push", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          BasicDBList currentValue;
          if (subObject.containsField(subKey)) {
            currentValue = expressionParser.typecast(subKey, subObject.get(subKey), BasicDBList.class);
          } else {
            currentValue = new BasicDBList();
          }

          if (object instanceof DBObject && (((DBObject) object).get("$each") != null)) {
            DBObject dbObject = (DBObject) object;
            Object eachObject = dbObject.get("$each");
            BasicDBList eachList = expressionParser.typecast(command + ".$each value", eachObject, BasicDBList.class);

            // position
            int pos = currentValue.size();
            Object positionObject = dbObject.get("$position");
            if (positionObject != null) {
              pos = expressionParser.typecast(command + ".$position value", positionObject, Number.class).intValue();
              if (pos >= currentValue.size()) {
                pos = currentValue.size();
              }
            }
            currentValue.addAll(pos, eachList);

            // sort
            Object sortObj = dbObject.get("$sort");
            if (sortObj != null) {
              if (sortObj instanceof Number) {
                int sortDirection = ((Number) sortObj).intValue();
                Collections.sort(currentValue, expressionParser.objectComparator(sortDirection));
              } else if (sortObj instanceof DBObject) {
                Collections.sort(currentValue, expressionParser.sortSpecificationComparator((DBObject) sortObj));
              }
            }

            // slice
            Object sliceObject = dbObject.get("$slice");
            if (sliceObject != null) {
              int slice = expressionParser.typecast(command + ".slice value", sliceObject, Number.class).intValue();
              if (slice == 0) {
                currentValue.clear();
                currentValue.trimToSize();
              } else if (slice > 0) {
                BasicDBList subList = new BasicDBList();
                subList.addAll(currentValue.subList(0, Math.min(slice, currentValue.size())));
                currentValue = subList;
              } else if (slice < 0 && currentValue.size() + slice >= 0) {
                BasicDBList subList = new BasicDBList();
                subList.addAll(currentValue.subList(Math.max(currentValue.size() + slice, 0), currentValue.size()));
                currentValue = subList;
              }
            }
          } else {
            currentValue.add(object);
          }

          subObject.put(subKey, currentValue);
        }
      },
      new BasicUpdate("$pushAll", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          List newList = expressionParser.typecast(command + " value", object, List.class);
          if (!subObject.containsField(subKey)) {
            subObject.put(subKey, newList);
          } else {
            BasicDBList currentValue = expressionParser.typecast(subKey, subObject.get(subKey), BasicDBList.class);
            currentValue.addAll(newList);
            subObject.put(subKey, currentValue);
          }
        }
      },
      new BasicUpdate("$addToSet", true) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          boolean isEach = false;
          BasicDBList currentValue = expressionParser.typecast(subKey, subObject.get(subKey), BasicDBList.class);
          currentValue = (currentValue == null) ? new BasicDBList() : currentValue;
          if (object instanceof DBObject) {
            Object eachObject = ((DBObject) object).get("$each");
            if (eachObject != null) {
              isEach = true;
              BasicDBList newList = expressionParser.typecast(command + ".$each value", eachObject, BasicDBList.class);
              if (newList == null) {
                throw new FongoException(command + ".$each must not be null");
              }
              for (Object newValue : newList) {
                if (!currentValue.contains(newValue)) {
                  currentValue.add(newValue);
                }
              }
            }
          }
          if (!isEach) {
            if (!currentValue.contains(object)) {
              currentValue.add(object);
            }
          }
          subObject.put(subKey, currentValue);
        }
      },
      new BasicUpdate("$pop", false) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          BasicDBList currentList = expressionParser.typecast(command, subObject.get(subKey), BasicDBList.class);
          if (currentList != null && currentList.size() > 0) {
            int direction = expressionParser.typecast(command, object, Number.class).intValue();
            if (direction > 0) {
              currentList.remove(currentList.size() - 1);
            } else {
              currentList.remove(0);
            }
          }
        }
      },
      new BasicUpdate("$pull", false) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          BasicDBList currentList = expressionParser.typecast(command + " only works on arrays", subObject.get(subKey), BasicDBList.class);
          if (currentList != null && currentList.size() > 0) {
            BasicDBList newList = new BasicDBList();
            if (object instanceof DBObject) {
              Filter filter = expressionParser.buildFilter((DBObject) object);
              for (Object item : currentList) {
                if (!(item instanceof DBObject) || !filter.apply((DBObject) item)) {
                  newList.add(item);
                }
              }
            } else {
              for (Object item : currentList) {
                if (!object.equals(item)) {
                  newList.add(item);
                }
              }
            }
            subObject.put(subKey, newList);
          }
        }
      },
      new BasicUpdate("$pullAll", false) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          BasicDBList currentList = expressionParser.typecast(command + " only works on arrays", subObject.get(subKey), BasicDBList.class);
          if (currentList != null && currentList.size() > 0) {
            Set pullSet = new HashSet(expressionParser.typecast(command, object, List.class));
            BasicDBList newList = new BasicDBList();
            for (Object item : currentList) {
              if (!pullSet.contains(item)) {
                newList.add(item);
              }
            }
            subObject.put(subKey, newList);
          }
        }
      },
      new BasicUpdate("$bit", false) {
        @Override
        void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
          Number currentNumber = expressionParser.typecast(command + " only works on integers", subObject.get(subKey), Number.class);
          if (currentNumber != null) {
            if (currentNumber instanceof Float || currentNumber instanceof Double) {
              throw new FongoException(command + " only works on integers");
            }
            DBObject bitOps = expressionParser.typecast(command, object, DBObject.class);
            for (String op : bitOps.keySet()) {
              Number opValue = expressionParser.typecast(command + "." + op, bitOps.get(op), Number.class);
              if ("and".equals(op)) {
                if (opValue instanceof Long || currentNumber instanceof Long) {
                  currentNumber = currentNumber.longValue() & opValue.longValue();
                } else {
                  currentNumber = currentNumber.intValue() & opValue.intValue();
                }
              } else if ("or".equals(op)) {
                if (opValue instanceof Long || currentNumber instanceof Long) {
                  currentNumber = currentNumber.longValue() | opValue.longValue();
                } else {
                  currentNumber = currentNumber.intValue() | opValue.intValue();
                }
              } else {
                throw new FongoException(command + "." + op + " is not valid.");
              }
            }
            subObject.put(subKey, currentNumber);
          }
        }
      }
  );
  final Map<String, BasicUpdate> commandMap = createCommandMap();
  private final BasicUpdate basicUpdateForUpsert = new BasicUpdate("upsert", true) {
    @Override
    void mergeAction(String subKey, DBObject subObject, Object object, DBObject objOriginal) {
      subObject.put(subKey, object);
    }
  };

  private Map<String, BasicUpdate> createCommandMap() {
    Map<String, BasicUpdate> map = new HashMap<String, BasicUpdate>();
    for (BasicUpdate item : commands) {
      map.put(item.command, item);
    }
    return map;
  }

  public DBObject doUpdate(final DBObject obj, final DBObject update) {
    return doUpdate(obj, update, new BasicDBObject());
  }

  public DBObject doUpdate(final DBObject obj, final DBObject update, DBObject query) {
    boolean updateDone = false;
    Set<String> seenKeys = new HashSet<String>();
    for (String command : update.keySet()) {
      BasicUpdate basicUpdate = commandMap.get(command);
      if (basicUpdate != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Doing update for command {}", command);
        }
        basicUpdate.doUpdate(obj, update, seenKeys, query);
        updateDone = true;
      } else if (command.startsWith("$")) {
        throw new FongoException("unsupported update: " + update);
      }
    }
    if (!updateDone) {
      for (Iterator<String> iter = obj.keySet().iterator(); iter.hasNext(); ) {
        String key = iter.next();
        if (key != "_id") {
          iter.remove();
        }
      }
      obj.putAll(update);
    }
    return obj;
  }

  public void mergeEmbeddedValueFromQuery(BasicDBObject newObject, DBObject q) {
    basicUpdateForUpsert.doUpdate(newObject, new BasicDBObject(basicUpdateForUpsert.command, q), new HashSet<String>(), q);
  }
}
