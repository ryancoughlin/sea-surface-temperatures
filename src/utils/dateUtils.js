export const formatDateToISO = (date) => {
    return date.toISOString().split('.')[0] + 'Z';
  };
  
  export const getTimeRange = () => {
    const endTime = new Date();
    endTime.setUTCHours(0, 0, 0, 0);
    endTime.setUTCDate(endTime.getUTCDate() - 1);
  
    const startTime = new Date(endTime);
    startTime.setUTCDate(startTime.getUTCDate() - 7);
  
    return {
      start: formatDateToISO(startTime),
      end: formatDateToISO(endTime)
    };
  };